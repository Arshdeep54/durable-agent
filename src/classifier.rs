#![allow(dead_code)]

use crate::domain::{Classification, Ticket};
use async_openai::Client;
use async_openai::config::OpenAIConfig;
use async_openai::error::OpenAIError;
use async_openai::types::chat::{
    ChatCompletionRequestMessage, ChatCompletionRequestSystemMessage,
    ChatCompletionRequestUserMessage, CreateChatCompletionRequest,
};
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

#[derive(Debug)]
pub struct ClassifyError {
    pub message: String,
    pub status: Option<u16>,
}

impl std::fmt::Display for ClassifyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.message.fmt(f)
    }
}

impl std::error::Error for ClassifyError {}

pub trait Classifier: Send + Sync {
    fn classify(
        &self,
        ticket: &Ticket,
    ) -> Pin<Box<dyn Future<Output = Result<Classification, ClassifyError>> + Send>>;
}

#[derive(Debug, Default)]
pub struct MockClassifier;

static FAIL_ONCE_USED: LazyLock<Mutex<HashSet<String>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

enum DemoClassifierMode {
    Normal,
    FailOnce,
    FailPermanent,
    Slow,
}

fn demo_classifier_mode_from_raw(raw: Option<&str>) -> DemoClassifierMode {
    match raw.map(str::trim) {
        None | Some("") | Some("normal") => DemoClassifierMode::Normal,
        Some("fail_once") => DemoClassifierMode::FailOnce,
        Some("fail_permanent") => DemoClassifierMode::FailPermanent,
        Some("slow") => DemoClassifierMode::Slow,
        _ => DemoClassifierMode::Normal,
    }
}

#[cfg(test)]
thread_local! {
    static DEMO_CLASSIFIER_MODE_TEST: std::cell::RefCell<Option<String>> =
        const { std::cell::RefCell::new(None) };
}

/// Reads `DEMO_CLASSIFIER_MODE` (demo-only; not a production knob).
fn demo_classifier_mode() -> DemoClassifierMode {
    #[cfg(test)]
    {
        if let Some(raw) = DEMO_CLASSIFIER_MODE_TEST.with(|mode| mode.borrow().clone()) {
            return demo_classifier_mode_from_raw(Some(raw.as_str()));
        }
    }
    demo_classifier_mode_from_raw(std::env::var("DEMO_CLASSIFIER_MODE").ok().as_deref())
}

fn classify_timeout_secs() -> u64 {
    std::env::var("CLASSIFY_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30)
}

fn mock_classify(body: &str) -> Classification {
    let lower = body.to_lowercase();
    let (category, urgency) = if lower.contains("refund") || lower.contains("charge") {
        ("billing", "high")
    } else if lower.contains("password") || lower.contains("login") {
        ("account", "high")
    } else {
        ("general", "normal")
    };

    Classification {
        category: category.to_string(),
        urgency: urgency.to_string(),
        draft_reply: format!("Thanks for reaching out about your {category} issue — we're on it."),
    }
}

fn openai_error_status(error: &OpenAIError) -> Option<u16> {
    match error {
        OpenAIError::ApiError(response) => Some(response.status_code.as_u16()),
        OpenAIError::Reqwest(error) => error.status().map(|status| status.as_u16()),
        _ => None,
    }
}

impl Classifier for MockClassifier {
    fn classify(
        &self,
        ticket: &Ticket,
    ) -> Pin<Box<dyn Future<Output = Result<Classification, ClassifyError>> + Send>> {
        let body = ticket.body.clone();
        let ticket_id = ticket.id.clone();
        Box::pin(async move {
            match demo_classifier_mode() {
                DemoClassifierMode::Normal => Ok(mock_classify(&body)),
                DemoClassifierMode::FailPermanent => Err(ClassifyError {
                    message: "demo permanent classify failure".into(),
                    status: Some(400),
                }),
                DemoClassifierMode::FailOnce => {
                    let fail_now = {
                        let mut used = FAIL_ONCE_USED.lock().expect("fail_once_used lock");
                        used.insert(ticket_id)
                    };
                    if fail_now {
                        return Err(ClassifyError {
                            message: "demo transient classify failure".into(),
                            status: Some(503),
                        });
                    }
                    Ok(mock_classify(&body))
                }
                DemoClassifierMode::Slow => {
                    let sleep_secs = classify_timeout_secs().saturating_add(5);
                    tokio::time::sleep(Duration::from_secs(sleep_secs)).await;
                    Ok(mock_classify(&body))
                }
            }
        })
    }
}

pub struct OpenAiClassifier {
    client: Client<OpenAIConfig>,
}

impl OpenAiClassifier {
    pub fn new() -> Self {
        Self {
            client: Client::with_config(OpenAIConfig::new()),
        }
    }
}

const SYSTEM_PROMPT: &str = r"You classify customer support tickets. Reply with a single JSON object only, no markdown, with keys: category (string), urgency (string), draft_reply (string).";

const OPENAI_REQUEST_TIMEOUT: Duration = Duration::from_secs(20);

const CLASSIFY_MALFORMED_RESPONSE_STATUS: u16 = 422;

impl Classifier for OpenAiClassifier {
    fn classify(
        &self,
        ticket: &Ticket,
    ) -> Pin<Box<dyn Future<Output = Result<Classification, ClassifyError>> + Send>> {
        let client = self.client.clone();
        let subject = ticket.subject.clone();
        let body = ticket.body.clone();

        Box::pin(async move {
            let user_content = format!("Subject: {subject}\n\n{body}");
            let request = CreateChatCompletionRequest {
                model: "gpt-4o-mini".into(),
                messages: vec![
                    ChatCompletionRequestMessage::System(ChatCompletionRequestSystemMessage::from(
                        SYSTEM_PROMPT,
                    )),
                    ChatCompletionRequestMessage::User(ChatCompletionRequestUserMessage::from(
                        user_content,
                    )),
                ],
                ..Default::default()
            };

            let response = tokio::time::timeout(OPENAI_REQUEST_TIMEOUT, async {
                client.chat().create(request).await
            })
            .await
            .map_err(|_| ClassifyError {
                message: format!(
                    "openai request timed out after {}s",
                    OPENAI_REQUEST_TIMEOUT.as_secs()
                ),
                status: None,
            })?
            .map_err(|e| ClassifyError {
                message: e.to_string(),
                status: openai_error_status(&e),
            })?;

            let content = response
                .choices
                .first()
                .and_then(|c| c.message.content.clone())
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(|| ClassifyError {
                    message: "empty model response".into(),
                    status: Some(CLASSIFY_MALFORMED_RESPONSE_STATUS),
                })?;

            serde_json::from_str::<Classification>(&content).map_err(|e| ClassifyError {
                message: format!("failed to parse classification JSON: {e}"),
                status: Some(CLASSIFY_MALFORMED_RESPONSE_STATUS),
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::approval::{MockApprovalSender, MockCustomerMailer};
    use crate::approval_correlations::ApprovalCorrelations;
    use crate::domain::Ticket;
    use crate::steps::build_step_bodies;
    use crate::ticket_system::InMemoryTicketSystem;
    use crate::workflow_def::ticket_workflow;
    use agentq::{DurableStore, Event, Priority, Queue, SqliteStore, WorkflowEngine};
    use std::sync::Arc;
    use std::sync::LazyLock;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    static DEMO_ENV_LOCK: LazyLock<std::sync::Mutex<()>> =
        LazyLock::new(|| std::sync::Mutex::new(()));

    struct DemoEnvGuard {
        saved: Vec<(String, Option<String>)>,
    }

    impl DemoEnvGuard {
        fn set(pairs: &[(&str, &str)]) -> Self {
            let _lock = DEMO_ENV_LOCK.lock().expect("demo env lock");
            let saved: Vec<_> = pairs
                .iter()
                .map(|(key, _)| ((*key).to_string(), std::env::var(key).ok()))
                .collect();
            for (key, value) in pairs {
                if *key == "DEMO_CLASSIFIER_MODE" {
                    DEMO_CLASSIFIER_MODE_TEST.with(|mode| {
                        *mode.borrow_mut() = Some((*value).to_string());
                    });
                } else {
                    unsafe { std::env::set_var(key, value) };
                }
            }
            Self { saved }
        }
    }

    impl Drop for DemoEnvGuard {
        fn drop(&mut self) {
            let _lock = DEMO_ENV_LOCK.lock().expect("demo env lock");
            DEMO_CLASSIFIER_MODE_TEST.with(|mode| *mode.borrow_mut() = None);
            for (key, prior) in self.saved.drain(..) {
                if key == "DEMO_CLASSIFIER_MODE" {
                    continue;
                }
                match prior {
                    Some(value) => unsafe { std::env::set_var(&key, value) },
                    None => unsafe { std::env::remove_var(&key) },
                }
            }
        }
    }

    fn sample_ticket(body: &str) -> Ticket {
        Ticket {
            id: "t1".into(),
            customer_id: "c1".into(),
            subject: "help".into(),
            body: body.into(),
        }
    }

    fn unique_ticket(id_prefix: &str) -> Ticket {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        Ticket {
            id: format!("{id_prefix}-{n}"),
            customer_id: "c1".into(),
            subject: "help".into(),
            body: "Just saying hello".into(),
        }
    }

    async fn run_classify(classifier: &dyn Classifier, ticket: &Ticket) -> Classification {
        classifier
            .classify(ticket)
            .await
            .expect("classify should succeed")
    }

    fn count_classify_retries(events: &[Event]) -> usize {
        events
            .iter()
            .filter(|e| matches!(e, Event::RetryScheduled { step_index: 1, .. }))
            .count()
    }

    fn classify_timeout_failures(events: &[Event]) -> usize {
        events
            .iter()
            .filter(|e| {
                matches!(
                    e,
                    Event::StepFailed {
                        step_index: 1,
                        reason,
                        ..
                    } if reason.contains("timed out")
                )
            })
            .count()
    }

    async fn workflow_engine_fixture(
        ticket: &Ticket,
        classifier: Arc<dyn Classifier>,
        classify_max_attempts: Option<u32>,
    ) -> (
        String,
        Arc<WorkflowEngine<SqliteStore>>,
        Arc<SqliteStore>,
        Arc<InMemoryTicketSystem>,
        Result<(), agentq::EngineError>,
    ) {
        static FIXTURE_ID: AtomicU64 = AtomicU64::new(0);
        let unique = FIXTURE_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "durable-agent-classifier-demo-{}-{}.db",
            std::process::id(),
            unique
        ));
        let path_str = path.to_str().expect("temp db path utf8").to_string();

        let store = SqliteStore::new(&path_str).expect("engine store");
        let reader_store = Arc::new(SqliteStore::new(&path_str).expect("reader store"));
        let queue = Queue::builder().start();
        let engine = Arc::new(WorkflowEngine::new(
            queue,
            store,
            "test-worker".to_string(),
            Duration::from_secs(60),
            Priority::High,
        ));
        let ticket_system = Arc::new(InMemoryTicketSystem::new());
        let correlations =
            Arc::new(ApprovalCorrelations::new(&path_str).expect("correlations store"));

        let bodies = build_step_bodies(
            ticket.clone(),
            classifier,
            Arc::new(MockApprovalSender),
            Arc::new(MockCustomerMailer),
            Arc::clone(&ticket_system) as Arc<dyn crate::ticket_system::TicketSystem>,
            Arc::clone(&engine),
            Arc::clone(&reader_store),
            correlations,
        );

        let run_result = {
            let mut workflow = ticket_workflow(&ticket.id);
            if let Some(max_attempts) = classify_max_attempts {
                workflow.steps[1].retry_policy.max_attempts = max_attempts;
            }
            engine.run(workflow, bodies).await
        };

        (path_str, engine, reader_store, ticket_system, run_result)
    }

    #[tokio::test]
    async fn mock_classifier_billing_keywords() {
        let c = MockClassifier;
        let result = run_classify(&c, &sample_ticket("I need a refund on my last charge")).await;
        assert_eq!(result.category, "billing");
        assert_eq!(result.urgency, "high");
        assert!(result.draft_reply.contains("billing"));
    }

    #[tokio::test]
    async fn mock_classifier_account_keywords() {
        let c = MockClassifier;
        let result = run_classify(&c, &sample_ticket("Can't reset my password or login")).await;
        assert_eq!(result.category, "account");
        assert_eq!(result.urgency, "high");
        assert!(result.draft_reply.contains("account"));
    }

    #[tokio::test]
    async fn mock_classifier_general_fallback() {
        let c = MockClassifier;
        let result = run_classify(&c, &sample_ticket("Just saying hello")).await;
        assert_eq!(result.category, "general");
        assert_eq!(result.urgency, "normal");
        assert!(result.draft_reply.contains("general"));
    }

    #[tokio::test]
    async fn demo_mode_slow_times_out_then_retries_classify_step() {
        let _env = DemoEnvGuard::set(&[
            ("DEMO_CLASSIFIER_MODE", "slow"),
            ("CLASSIFY_TIMEOUT_SECS", "1"),
        ]);
        let ticket = unique_ticket("demo-slow");
        let classifier: Arc<dyn Classifier> = Arc::new(MockClassifier);
        let (path, _engine, reader_store, _, run_result) =
            workflow_engine_fixture(&ticket, Arc::clone(&classifier), Some(2)).await;
        assert!(
            matches!(run_result, Err(agentq::EngineError::WorkflowFailed { .. })),
            "slow classify should exhaust step timeouts and fail the workflow"
        );

        let events = reader_store.load_events(&ticket.id).expect("load events");
        assert!(
            classify_timeout_failures(&events) >= 1,
            "expected at least one classify timeout failure in events: {events:?}"
        );
        assert_eq!(
            count_classify_retries(&events),
            1,
            "expected exactly one retry after the first classify timeout"
        );

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn demo_mode_fail_once_retries_once_then_completes() {
        let _env = DemoEnvGuard::set(&[("DEMO_CLASSIFIER_MODE", "fail_once")]);
        let ticket = unique_ticket("demo-fail-once");
        let classifier: Arc<dyn Classifier> = Arc::new(MockClassifier);
        let (path, engine, reader_store, ticket_system, run_result) =
            workflow_engine_fixture(&ticket, Arc::clone(&classifier), None).await;
        run_result.expect("fail_once workflow should reach approval wait");

        let events = reader_store
            .load_events(&ticket.id)
            .expect("events before resume");
        assert_eq!(
            count_classify_retries(&events),
            1,
            "fail_once should schedule exactly one classify retry"
        );

        engine
            .resume(&ticket.id, 2, "approved".to_string())
            .await
            .expect("resume after approval");

        let events = reader_store
            .load_events(&ticket.id)
            .expect("events after completion");
        assert!(
            events
                .iter()
                .any(|e| matches!(e, Event::WorkflowCompleted { .. })),
            "workflow should complete after fail_once recovery"
        );
        assert!(ticket_system.is_resolved(&ticket.id));

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn openai_error_status_maps_api_and_other_errors() {
        use async_openai::error::{ApiError, ApiErrorResponse};
        use reqwest::StatusCode;

        let api = |status: u16| {
            OpenAIError::ApiError(ApiErrorResponse {
                status_code: StatusCode::from_u16(status).expect("status"),
                api_error: ApiError {
                    message: "test".into(),
                    r#type: None,
                    param: None,
                    code: None,
                    misalignment: None,
                },
            })
        };

        assert_eq!(openai_error_status(&api(400)), Some(400));
        assert_eq!(openai_error_status(&api(429)), Some(429));
        assert_eq!(openai_error_status(&api(503)), Some(503));
        assert_eq!(
            openai_error_status(&OpenAIError::InvalidArgument("bad args".into())),
            None
        );
    }

    struct MalformedResponseClassifier;

    impl Classifier for MalformedResponseClassifier {
        fn classify(
            &self,
            _ticket: &Ticket,
        ) -> Pin<Box<dyn Future<Output = Result<Classification, ClassifyError>> + Send>> {
            Box::pin(async move {
                Err(ClassifyError {
                    message: "empty model response".into(),
                    status: Some(CLASSIFY_MALFORMED_RESPONSE_STATUS),
                })
            })
        }
    }

    #[tokio::test]
    async fn malformed_classify_response_does_not_retry() {
        let ticket = unique_ticket("malformed-classify");
        let classifier: Arc<dyn Classifier> = Arc::new(MalformedResponseClassifier);
        let (path, _engine, reader_store, _ticket_system, run_result) =
            workflow_engine_fixture(&ticket, classifier, None).await;
        assert!(
            matches!(run_result, Err(agentq::EngineError::WorkflowFailed { .. })),
            "malformed classify response should fail the workflow without retries"
        );

        let events = reader_store.load_events(&ticket.id).expect("load events");
        assert_eq!(
            count_classify_retries(&events),
            0,
            "malformed classify response must not schedule classify retries"
        );

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn demo_mode_fail_permanent_does_not_retry() {
        let _env = DemoEnvGuard::set(&[("DEMO_CLASSIFIER_MODE", "fail_permanent")]);
        let ticket = unique_ticket("demo-fail-permanent");
        let classifier: Arc<dyn Classifier> = Arc::new(MockClassifier);
        let (path, _engine, reader_store, _ticket_system, run_result) =
            workflow_engine_fixture(&ticket, classifier, None).await;
        assert!(
            matches!(run_result, Err(agentq::EngineError::WorkflowFailed { .. })),
            "fail_permanent should fail the workflow without retries"
        );

        let events = reader_store.load_events(&ticket.id).expect("load events");
        assert!(
            events
                .iter()
                .any(|e| matches!(e, Event::WorkflowFailed { .. })),
            "workflow should fail on non-retryable classify error"
        );
        assert_eq!(
            count_classify_retries(&events),
            0,
            "non-retryable demo failure must not schedule classify retries"
        );

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    #[ignore = "requires OPENAI_API_KEY and network"]
    async fn openai_classifier_returns_parsed_classification() {
        let classifier = OpenAiClassifier::new();
        let ticket = sample_ticket("My card was charged twice this month.");
        let result = classifier.classify(&ticket).await.expect("openai classify");

        assert!(!result.category.is_empty());
        assert!(!result.urgency.is_empty());
        assert!(!result.draft_reply.is_empty());
    }
}
