//! Deterministic reliability scenarios for the durable execution engine.
//!
//! Pass/fail comes from ordinary Rust assertions on engine behavior (step counts,
//! terminal status, idempotency). Outcomes are forwarded to Respan as labeled spans
//! for visibility only; Respan's LLM-judge evaluation product is not used to grade
//! whether the engine ran steps the expected number of times.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use agentq::{
    Backoff, DurableStore, EngineError, Event, Priority, Queue, RetryPolicy, SqliteStore, StepDef,
    StepFunc, Workflow, WorkflowEngine,
};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use crate::api::{ApiState, router};
use crate::classifier::{Classifier, ClassifyError, MockClassifier};
use crate::domain::{Classification, Ticket};
use crate::registry::WorkflowRegistry;
use crate::ticket_system::{InMemoryTicketSystem, TicketSystem};
use crate::tracing_sink::{NoopSink, RespanSink, TraceSink, TraceSpan};
use crate::webhook::{compute_svix_signature, decode_signing_secret};
use crate::workflow_def::ticket_workflow;

const WEBHOOK_TEST_SECRET: &str = "whsec_aGVsbG8=";

fn eval_db_path(label: &str) -> String {
    std::env::temp_dir()
        .join(format!(
            "durable-agent-reliability-eval-{}-{}.db",
            std::process::id(),
            label
        ))
        .to_str()
        .expect("temp db path utf8")
        .to_string()
}

fn eval_trace_sink() -> Arc<dyn TraceSink> {
    match std::env::var("RESPAN_API_KEY")
        .ok()
        .filter(|s| !s.is_empty())
    {
        Some(key) => Arc::new(RespanSink::new(key)),
        None => Arc::new(NoopSink),
    }
}

fn record_eval_pass(sink: &Arc<dyn TraceSink>, scenario: &str) {
    let metadata = serde_json::json!({
        "scenario": scenario,
        "result": "pass",
    });
    sink.record_batch(vec![TraceSpan {
        trace_id: format!("eval-{scenario}"),
        span_id: format!("eval-{scenario}-pass"),
        parent_span_id: None,
        path: format!("eval/{scenario}"),
        name: format!("eval: {scenario}"),
        log_type: "task",
        output: metadata.to_string(),
        metadata,
    }]);
}

fn eval_state(path: &str) -> ApiState {
    ApiState::new_with_trace_sink(path, eval_trace_sink())
}

fn sample_ticket(id: &str) -> Ticket {
    Ticket {
        id: id.to_string(),
        customer_id: "customer@example.com".to_string(),
        subject: "Need help".to_string(),
        body: "Just saying hello".to_string(),
    }
}

async fn poll_http_status(
    app: &axum::Router,
    id: &str,
    want: &str,
    max_attempts: u32,
) -> serde_json::Value {
    for _ in 0..max_attempts {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/workflows/{id}"))
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("oneshot");
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let view: serde_json::Value = serde_json::from_slice(&body).expect("json");
        if view["status"] == want {
            return view;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("workflow {id} did not reach status {want} within {max_attempts} attempts");
}

fn workflow_completed(events: &[Event]) -> bool {
    events
        .iter()
        .any(|e| matches!(e, Event::WorkflowCompleted { .. }))
}

fn workflow_failed(events: &[Event]) -> bool {
    events
        .iter()
        .any(|e| matches!(e, Event::WorkflowFailed { .. }))
}

fn count_step_waiting(events: &[Event]) -> usize {
    events
        .iter()
        .filter(|e| matches!(e, Event::StepWaiting { .. }))
        .count()
}

fn count_step_resumed(events: &[Event], step_index: usize) -> usize {
    events
        .iter()
        .filter(|e| {
            matches!(
                e,
                Event::StepResumed {
                    step_index: si, ..
                } if *si == step_index
            )
        })
        .count()
}

fn seed_expired_lease(path: &str, workflow_id: &str, step_index: usize) {
    let expired = SystemTime::now()
        .checked_sub(Duration::from_secs(120))
        .unwrap_or(UNIX_EPOCH);
    let since_epoch = expired.duration_since(UNIX_EPOCH).expect("epoch");
    let expires_at_secs = since_epoch.as_secs() as i64;
    let expires_at_nanos = since_epoch.subsec_nanos() as i64;

    let conn = rusqlite::Connection::open(path).expect("open seed db");
    conn.execute(
        "INSERT INTO steps (workflow_id, step_index, status, worker_id, expires_at_secs, expires_at_nanos, output, reason, attempt)
         VALUES (?1, ?2, 'leased', 'dead-worker', ?3, ?4, NULL, NULL, 0)
         ON CONFLICT(workflow_id, step_index) DO UPDATE SET
           status = 'leased',
           worker_id = excluded.worker_id,
           expires_at_secs = excluded.expires_at_secs,
           expires_at_nanos = excluded.expires_at_nanos,
           output = NULL,
           reason = NULL,
           attempt = excluded.attempt",
        rusqlite::params![workflow_id, step_index as i64, expires_at_secs, expires_at_nanos],
    )
    .expect("seed expired lease");
}

fn seed_crash_mid_send_reply(path: &str, workflow_id: &str, classification_json: &str) {
    let store = SqliteStore::new(path).expect("seed store");
    store
        .append_event(&Event::WorkflowStarted {
            workflow_id: workflow_id.to_string(),
        })
        .expect("WorkflowStarted");
    for (step_index, output) in [
        (0usize, workflow_id.to_string()),
        (1, classification_json.to_string()),
        (2, "approved reply text".to_string()),
    ] {
        store
            .append_event(&Event::StepCompleted {
                workflow_id: workflow_id.to_string(),
                step_index,
                output,
            })
            .expect("StepCompleted");
    }
    seed_expired_lease(path, workflow_id, 3);
}

fn seed_crash_before_workflow_completed(path: &str, workflow_id: &str, classification_json: &str) {
    let store = SqliteStore::new(path).expect("seed store");
    store
        .append_event(&Event::WorkflowStarted {
            workflow_id: workflow_id.to_string(),
        })
        .expect("WorkflowStarted");
    for (step_index, output) in [
        (0usize, workflow_id.to_string()),
        (1, classification_json.to_string()),
        (2, "approved reply text".to_string()),
        (3, "approved reply text".to_string()),
        (4, "resolved".to_string()),
    ] {
        store
            .append_event(&Event::StepCompleted {
                workflow_id: workflow_id.to_string(),
                step_index,
                output,
            })
            .expect("StepCompleted");
    }
    seed_expired_lease(path, workflow_id, 5);
}

async fn poll_workflow_completed(path: &str, workflow_id: &str) {
    let reader = SqliteStore::new(path).expect("poll store");
    for _ in 0..200 {
        let events = reader
            .load_events(workflow_id)
            .expect("load_events while polling");
        if workflow_completed(&events) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let events = reader.load_events(workflow_id).expect("final events");
    panic!("workflow did not reach WorkflowCompleted; last events: {events:?}");
}

struct FlakyClassifier {
    calls: AtomicUsize,
}

impl FlakyClassifier {
    fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
        }
    }
}

impl Classifier for FlakyClassifier {
    fn classify(
        &self,
        ticket: &Ticket,
    ) -> Pin<Box<dyn Future<Output = Result<Classification, ClassifyError>> + Send>> {
        let body = ticket.body.clone();
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if n == 0 {
                return Err(ClassifyError {
                    message: "transient upstream failure".into(),
                    status: Some(503),
                });
            }
            let classification = MockClassifier
                .classify(&Ticket {
                    id: "unused".into(),
                    customer_id: "c@example.com".into(),
                    subject: "s".into(),
                    body,
                })
                .await
                .expect("mock classify");
            Ok(classification)
        })
    }
}

struct PermanentFailClassifier;

impl Classifier for PermanentFailClassifier {
    fn classify(
        &self,
        _ticket: &Ticket,
    ) -> Pin<Box<dyn Future<Output = Result<Classification, ClassifyError>> + Send>> {
        Box::pin(async {
            Err(ClassifyError {
                message: "bad request".into(),
                status: Some(400),
            })
        })
    }
}

fn sign_webhook_payload(svix_id: &str, svix_timestamp: &str, body: &[u8]) -> String {
    let secret_bytes = decode_signing_secret(WEBHOOK_TEST_SECRET).expect("secret");
    let sig = compute_svix_signature(&secret_bytes, svix_id, svix_timestamp, body);
    format!("v1,{sig}")
}

fn webhook_request(
    body: &str,
    svix_id: &str,
    svix_timestamp: &str,
    signature: &str,
) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/webhooks/agentmail")
        .header("content-type", "application/json")
        .header("svix-id", svix_id)
        .header("svix-timestamp", svix_timestamp)
        .header("svix-signature", signature)
        .body(Body::from(body.to_string()))
        .expect("request")
}

async fn seed_waiting_workflow_for_webhook(
    path: &str,
    workflow_id: &str,
    thread_id: &str,
) -> Arc<WorkflowEngine<SqliteStore>> {
    use crate::approval::{MockApprovalSender, MockCustomerMailer};
    use crate::approval_correlations::ApprovalCorrelations;
    use crate::steps::build_step_bodies;

    let ticket = sample_ticket(workflow_id);
    let registry = WorkflowRegistry::new(path).expect("registry");
    registry.insert(&ticket).expect("insert");

    let store = SqliteStore::new(path).expect("store");
    store
        .append_event(&Event::WorkflowStarted {
            workflow_id: workflow_id.to_string(),
        })
        .expect("started");
    for (step_index, output) in [(0usize, workflow_id.to_string()), (1, "{}".to_string())] {
        store
            .append_event(&Event::StepCompleted {
                workflow_id: workflow_id.to_string(),
                step_index,
                output,
            })
            .expect("completed");
    }
    store
        .append_event(&Event::StepWaiting {
            workflow_id: workflow_id.to_string(),
            step_index: 2,
            reason: "awaiting human approval".into(),
        })
        .expect("waiting");

    let correlations = ApprovalCorrelations::new(path).expect("correlations");
    correlations
        .insert(thread_id, workflow_id, 2)
        .expect("correlation");

    let reader_store = Arc::new(SqliteStore::new(path).expect("reader"));
    let engine_store = SqliteStore::new(path).expect("engine store");
    let queue = Queue::builder().start();
    let engine = Arc::new(WorkflowEngine::new(
        queue,
        engine_store,
        "eval-webhook-worker".into(),
        Duration::from_secs(60),
        Priority::High,
    ));

    let bodies = build_step_bodies(
        ticket,
        Arc::new(MockClassifier),
        Arc::new(MockApprovalSender),
        Arc::new(MockCustomerMailer),
        Arc::new(InMemoryTicketSystem::new()),
        Arc::clone(&engine),
        reader_store,
        Arc::new(correlations),
    );
    engine
        .register_workflow(ticket_workflow(workflow_id), bodies)
        .expect("register");

    engine
}

#[tokio::test]
async fn eval_normal_completion() {
    let path = eval_db_path("normal-completion");
    let _ = std::fs::remove_file(&path);
    let sink = eval_trace_sink();
    let app = router().with_state(eval_state(&path));
    let workflow_id = "eval-normal";

    let ticket_json = format!(
        r#"{{
            "id": "{workflow_id}",
            "customer_id": "cust@example.com",
            "subject": "Need help",
            "body": "Just saying hello"
        }}"#
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/workflows")
                .header("content-type", "application/json")
                .body(Body::from(ticket_json))
                .expect("request"),
        )
        .await
        .expect("oneshot");
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/workflows/{workflow_id}/run"))
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("oneshot");
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    poll_http_status(&app, workflow_id, "waiting", 200).await;

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/workflows/{workflow_id}/approve"))
                .body(Body::from("approved reply text"))
                .expect("request"),
        )
        .await
        .expect("oneshot");
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    poll_http_status(&app, workflow_id, "completed", 200).await;
    record_eval_pass(&sink, "normal_completion");
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn eval_worker_crash_during_step() {
    let path = eval_db_path("crash-during-step");
    let _ = std::fs::remove_file(&path);
    let sink = eval_trace_sink();

    let workflow_id = "eval-crash-step";
    let ticket = sample_ticket(workflow_id);
    let classification = MockClassifier
        .classify(&ticket)
        .await
        .expect("mock classify");
    let classification_json = serde_json::to_string(&classification).expect("classification json");

    {
        let registry = WorkflowRegistry::new(&path).expect("registry");
        registry.insert(&ticket).expect("insert ticket");
        seed_crash_mid_send_reply(&path, workflow_id, &classification_json);
    }

    let state = eval_state(&path);
    let recovered = state
        .recover_pending_workflows()
        .await
        .expect("recover_pending_workflows");
    assert!(recovered >= 1, "expected at least one re-admitted step");

    poll_workflow_completed(&path, workflow_id).await;
    record_eval_pass(&sink, "worker_crash_during_step");
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn eval_crash_before_completion_persistence() {
    let path = eval_db_path("crash-before-complete");
    let _ = std::fs::remove_file(&path);
    let sink = eval_trace_sink();

    let workflow_id = "eval-crash-complete";
    let ticket = sample_ticket(workflow_id);
    let classification = MockClassifier
        .classify(&ticket)
        .await
        .expect("mock classify");
    let classification_json = serde_json::to_string(&classification).expect("classification json");

    {
        let registry = WorkflowRegistry::new(&path).expect("registry");
        registry.insert(&ticket).expect("insert ticket");
        seed_crash_before_workflow_completed(&path, workflow_id, &classification_json);
    }

    let state = eval_state(&path);
    let recovered = state
        .recover_pending_workflows()
        .await
        .expect("recover_pending_workflows");
    assert!(recovered >= 1, "expected at least one re-admitted step");

    poll_workflow_completed(&path, workflow_id).await;
    record_eval_pass(&sink, "crash_before_completion_persistence");
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn eval_retryable_failure() {
    let path = eval_db_path("retryable");
    let _ = std::fs::remove_file(&path);
    let sink = eval_trace_sink();
    let app = router().with_state(ApiState::new_with_classifier_and_trace_sink(
        &path,
        Arc::new(FlakyClassifier::new()),
        Arc::clone(&sink),
    ));
    let workflow_id = "eval-retry";

    let ticket_json = format!(
        r#"{{
            "id": "{workflow_id}",
            "customer_id": "cust@example.com",
            "subject": "Need help",
            "body": "Just saying hello"
        }}"#
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/workflows")
                .header("content-type", "application/json")
                .body(Body::from(ticket_json))
                .expect("request"),
        )
        .await
        .expect("oneshot");
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/workflows/{workflow_id}/run"))
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("oneshot");
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    poll_http_status(&app, workflow_id, "waiting", 200).await;

    let reader = SqliteStore::new(&path).expect("reader");
    let events = reader.load_events(workflow_id).expect("events");
    let retries = events
        .iter()
        .filter(|e| matches!(e, Event::RetryScheduled { .. }))
        .count();
    assert!(
        retries >= 1,
        "expected at least one retry after transient failure"
    );

    record_eval_pass(&sink, "retryable_failure");
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn eval_non_retryable_failure() {
    let path = eval_db_path("non-retryable");
    let _ = std::fs::remove_file(&path);
    let sink = eval_trace_sink();
    let app = router().with_state(ApiState::new_with_classifier_and_trace_sink(
        &path,
        Arc::new(PermanentFailClassifier),
        Arc::clone(&sink),
    ));
    let workflow_id = "eval-non-retry";

    let ticket_json = format!(
        r#"{{
            "id": "{workflow_id}",
            "customer_id": "cust@example.com",
            "subject": "Need help",
            "body": "Just saying hello"
        }}"#
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/workflows")
                .header("content-type", "application/json")
                .body(Body::from(ticket_json))
                .expect("request"),
        )
        .await
        .expect("oneshot");
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/workflows/{workflow_id}/run"))
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("oneshot");
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    poll_http_status(&app, workflow_id, "failed", 200).await;

    let reader = SqliteStore::new(&path).expect("reader");
    let events = reader.load_events(workflow_id).expect("events");
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, Event::RetryScheduled { .. })),
        "non-retryable classify failure must not schedule retries"
    );

    record_eval_pass(&sink, "non_retryable_failure");
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn eval_timeout() {
    let path = eval_db_path("timeout");
    let _ = std::fs::remove_file(&path);
    let sink = eval_trace_sink();
    let state = eval_state(&path);

    let workflow_id = "eval-timeout";
    let body: StepFunc = Box::new(|| Box::pin(std::future::pending()));
    let workflow = Workflow {
        id: workflow_id.to_string(),
        steps: vec![StepDef {
            name: "hang".to_string(),
            retry_policy: RetryPolicy {
                max_attempts: 2,
                backoff: Backoff::Fixed(Duration::from_millis(1)),
            },
            timeout: Some(Duration::from_millis(20)),
        }],
    };

    let engine = Arc::clone(&state.engine);
    let result = engine.run(workflow, vec![body]).await;
    assert!(
        matches!(result, Err(EngineError::WorkflowFailed { .. })),
        "expected workflow failure after step timeouts exhaust retries"
    );

    let reader = SqliteStore::new(&path).expect("reader");
    let events = reader.load_events(workflow_id).expect("events");
    assert!(!workflow_completed(&events));
    assert!(workflow_failed(&events));
    // max_attempts: 2 with a step that only ever times out means the first
    // timeout must be followed by a scheduled retry before the second
    // timeout exhausts attempts and fails the workflow.
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, Event::RetryScheduled { .. }))
            .count(),
        1,
        "expected exactly one retry scheduled between the two timeouts"
    );

    record_eval_pass(&sink, "timeout");
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn eval_duplicate_run_while_waiting() {
    let path = eval_db_path("duplicate-run");
    let _ = std::fs::remove_file(&path);
    let sink = eval_trace_sink();
    let app = router().with_state(eval_state(&path));
    let workflow_id = "eval-dup-run";

    let ticket_json = format!(
        r#"{{
            "id": "{workflow_id}",
            "customer_id": "cust@example.com",
            "subject": "Need help",
            "body": "Just saying hello"
        }}"#
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/workflows")
                .header("content-type", "application/json")
                .body(Body::from(ticket_json))
                .expect("request"),
        )
        .await
        .expect("oneshot");
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/workflows/{workflow_id}/run"))
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("oneshot");
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    poll_http_status(&app, workflow_id, "waiting", 200).await;

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/workflows/{workflow_id}/run"))
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("oneshot");
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let reader = SqliteStore::new(&path).expect("reader");
    let events_before = reader.load_events(workflow_id).expect("events");
    assert_eq!(
        count_step_waiting(&events_before),
        1,
        "duplicate run must not create a second waiting pause"
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/workflows/{workflow_id}/approve"))
                .body(Body::from("approved reply text"))
                .expect("request"),
        )
        .await
        .expect("oneshot");
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    poll_http_status(&app, workflow_id, "completed", 200).await;
    record_eval_pass(&sink, "duplicate_run_while_waiting");
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn eval_duplicate_approval_webhook() {
    let path = eval_db_path("duplicate-webhook");
    let _ = std::fs::remove_file(&path);
    let sink = eval_trace_sink();
    let thread_id = "thd_eval_dup";
    let workflow_id = "eval-dup-webhook";
    let engine = seed_waiting_workflow_for_webhook(&path, workflow_id, thread_id).await;

    let mut state = ApiState::new_with_webhook_secret_and_trace_sink(
        &path,
        WEBHOOK_TEST_SECRET,
        Arc::clone(&sink),
    );
    state.engine = engine;

    let body = format!(
        r#"{{"event_type":"message.received","message":{{"thread_id":"{thread_id}","text":"approved"}}}}"#,
    );
    let svix_id = "svix_eval_dup";
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("epoch")
        .as_secs()
        .to_string();
    let sig = sign_webhook_payload(svix_id, &ts, body.as_bytes());

    let app = router().with_state(state);
    let r1 = app
        .clone()
        .oneshot(webhook_request(&body, svix_id, &ts, &sig))
        .await
        .expect("first");
    assert_eq!(r1.status(), StatusCode::OK);
    let r2 = app
        .clone()
        .oneshot(webhook_request(&body, svix_id, &ts, &sig))
        .await
        .expect("second");
    assert_eq!(r2.status(), StatusCode::OK);

    for _ in 0..200 {
        let reader = SqliteStore::new(&path).expect("reader");
        let events = reader.load_events(workflow_id).expect("events");
        if workflow_completed(&events) {
            assert_eq!(count_step_resumed(&events, 2), 1);
            record_eval_pass(&sink, "duplicate_approval_webhook");
            let _ = std::fs::remove_file(&path);
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("workflow did not complete after duplicate webhook");
}

#[tokio::test]
async fn eval_human_approval() {
    let path = eval_db_path("human-approve");
    let _ = std::fs::remove_file(&path);
    let sink = eval_trace_sink();
    let app = router().with_state(eval_state(&path));
    let workflow_id = "eval-approve";

    let ticket_json = format!(
        r#"{{
            "id": "{workflow_id}",
            "customer_id": "cust@example.com",
            "subject": "Need help",
            "body": "Just saying hello"
        }}"#
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/workflows")
                .header("content-type", "application/json")
                .body(Body::from(ticket_json))
                .expect("request"),
        )
        .await
        .expect("oneshot");
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/workflows/{workflow_id}/run"))
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("oneshot");
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    poll_http_status(&app, workflow_id, "waiting", 200).await;

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/workflows/{workflow_id}/approve"))
                .body(Body::from("human approved text"))
                .expect("request"),
        )
        .await
        .expect("oneshot");
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    poll_http_status(&app, workflow_id, "completed", 200).await;
    record_eval_pass(&sink, "human_approval");
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn eval_human_rejection() {
    let path = eval_db_path("human-reject");
    let _ = std::fs::remove_file(&path);
    let sink = eval_trace_sink();
    let app = router().with_state(eval_state(&path));
    let workflow_id = "eval-reject";

    let ticket_json = format!(
        r#"{{
            "id": "{workflow_id}",
            "customer_id": "cust@example.com",
            "subject": "Need help",
            "body": "Just saying hello"
        }}"#
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/workflows")
                .header("content-type", "application/json")
                .body(Body::from(ticket_json))
                .expect("request"),
        )
        .await
        .expect("oneshot");
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/workflows/{workflow_id}/run"))
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("oneshot");
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    poll_http_status(&app, workflow_id, "waiting", 200).await;

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/workflows/{workflow_id}/reject"))
                .body(Body::from("not needed"))
                .expect("request"),
        )
        .await
        .expect("oneshot");
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    poll_http_status(&app, workflow_id, "failed", 200).await;
    record_eval_pass(&sink, "human_rejection");
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn eval_restart_while_waiting() {
    let path = eval_db_path("restart-waiting");
    let _ = std::fs::remove_file(&path);
    let sink = eval_trace_sink();
    let app = router().with_state(eval_state(&path));
    let workflow_id = "eval-restart-wait";

    let ticket_json = format!(
        r#"{{
            "id": "{workflow_id}",
            "customer_id": "cust@example.com",
            "subject": "Need help",
            "body": "Just saying hello"
        }}"#
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/workflows")
                .header("content-type", "application/json")
                .body(Body::from(ticket_json))
                .expect("request"),
        )
        .await
        .expect("oneshot");
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/workflows/{workflow_id}/run"))
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("oneshot");
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    poll_http_status(&app, workflow_id, "waiting", 200).await;

    let restarted = eval_state(&path);
    restarted
        .recover_pending_workflows()
        .await
        .expect("recover after restart");

    let app2 = router().with_state(restarted);
    let view = poll_http_status(&app2, workflow_id, "waiting", 50).await;
    assert_eq!(view["waiting_step"], 2);

    let response = app2
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/workflows/{workflow_id}/approve"))
                .body(Body::from("after restart"))
                .expect("request"),
        )
        .await
        .expect("oneshot");
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    poll_http_status(&app2, workflow_id, "completed", 200).await;
    record_eval_pass(&sink, "restart_while_waiting");
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn eval_ticket_system_idempotency() {
    let path = eval_db_path("ticket-idempotent");
    let _ = std::fs::remove_file(&path);
    let sink = eval_trace_sink();
    let ticket_system = Arc::new(InMemoryTicketSystem::new());
    let ticket_system_check = Arc::clone(&ticket_system);
    let app = router().with_state(ApiState::new_with_ticket_system_and_trace_sink(
        &path,
        ticket_system,
        Arc::clone(&sink),
    ));
    let workflow_id = "eval-ticket-idem";

    let ticket_json = format!(
        r#"{{
            "id": "{workflow_id}",
            "customer_id": "cust@example.com",
            "subject": "Need help",
            "body": "Just saying hello"
        }}"#
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/workflows")
                .header("content-type", "application/json")
                .body(Body::from(ticket_json))
                .expect("request"),
        )
        .await
        .expect("oneshot");
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/workflows/{workflow_id}/run"))
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("oneshot");
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    poll_http_status(&app, workflow_id, "waiting", 200).await;

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/workflows/{workflow_id}/approve"))
                .body(Body::from("approved reply text"))
                .expect("request"),
        )
        .await
        .expect("oneshot");
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    poll_http_status(&app, workflow_id, "completed", 200).await;
    assert!(ticket_system_check.is_resolved(workflow_id));

    ticket_system_check
        .mark_resolved(workflow_id)
        .await
        .expect("second mark_resolved should succeed");
    assert!(ticket_system_check.is_resolved(workflow_id));

    record_eval_pass(&sink, "ticket_system_idempotency");
    let _ = std::fs::remove_file(&path);
}
