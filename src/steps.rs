use std::sync::Arc;

use agentq::{DurableStore, Event, NonRetryable, StepFunc, WaitForInput, WorkflowEngine};

use crate::approval::{ApprovalSender, CustomerMailer};
use crate::classifier::Classifier;
use crate::domain::{Classification, Ticket};
use crate::ticket_system::TicketSystem;

fn is_non_retryable_status(status: Option<u16>) -> bool {
    matches!(status, Some(code) if (400..500).contains(&code) && code != 429)
}

fn http_adapter_step_error<E>(err: E, status: Option<u16>) -> Box<dyn std::error::Error + Send + Sync>
where
    E: std::error::Error + Send + Sync + 'static,
{
    if is_non_retryable_status(status) {
        Box::new(NonRetryable(err.to_string()))
    } else {
        Box::new(err)
    }
}

fn approval_resume_output(input: String) -> Result<String, NonRetryable> {
    let trimmed = input.trim();
    if trimmed.eq_ignore_ascii_case("rejected") {
        return Err(NonRetryable("rejected".to_string()));
    }
    const PREFIX: &str = "rejected:";
    if trimmed.len() >= PREFIX.len()
        && trimmed[..PREFIX.len()].eq_ignore_ascii_case(PREFIX)
    {
        let reason = trimmed[PREFIX.len()..].trim();
        let message = if reason.is_empty() {
            "rejected".to_string()
        } else {
            reason.to_string()
        };
        return Err(NonRetryable(message));
    }
    Ok(input)
}

fn step_completed_output(events: &[Event], step_index: usize) -> Option<String> {
    events
        .iter()
        .rev()
        .find_map(|event| match event {
            Event::StepCompleted {
                step_index: si,
                output,
                ..
            } if *si == step_index => Some(output.clone()),
            _ => None,
        })
}

pub fn build_step_bodies(
    ticket: Ticket,
    classifier: Arc<dyn Classifier>,
    approval_sender: Arc<dyn ApprovalSender>,
    mailer: Arc<dyn CustomerMailer>,
    ticket_system: Arc<dyn TicketSystem>,
    engine: Arc<WorkflowEngine<agentq::SqliteStore>>,
    reader_store: Arc<agentq::SqliteStore>,
) -> Vec<StepFunc> {
    let ticket_ingest = ticket.clone();
    let ingest: StepFunc = Box::new(move || {
        let id = ticket_ingest.id.clone();
        Box::pin(async move { Ok(id) })
    });

    let ticket_classify = ticket.clone();
    let classifier_classify = Arc::clone(&classifier);
    let classify: StepFunc = Box::new(move || {
        let ticket = ticket_classify.clone();
        let classifier = Arc::clone(&classifier_classify);
        Box::pin(async move {
            let classification = classifier.classify(&ticket).await.map_err(|e| {
                let status = e.status;
                http_adapter_step_error(e, status)
            })?;
            Ok(serde_json::to_string(&classification)?)
        })
    });

    let ticket_approval = ticket.clone();
    let approval_sender_approval = Arc::clone(&approval_sender);
    let engine_approval = Arc::clone(&engine);
    let reader_store_approval = Arc::clone(&reader_store);
    let request_approval: StepFunc = Box::new(move || {
        let ticket = ticket_approval.clone();
        let approval_sender = Arc::clone(&approval_sender_approval);
        let engine = Arc::clone(&engine_approval);
        let reader_store = Arc::clone(&reader_store_approval);
        Box::pin(async move {
            if let Some(input) = engine.resume_input(&ticket.id, 2) {
                return approval_resume_output(input).map_err(|e| {
                    Box::new(e) as Box<dyn std::error::Error + Send + Sync>
                });
            }

            let events = reader_store.load_events(&ticket.id)?;
            let classification_json = step_completed_output(&events, 1)
                .ok_or("classification output missing")?;
            let classification: Classification = serde_json::from_str(&classification_json)?;
            approval_sender
                .request_approval(&ticket, &classification.draft_reply)
                .await
                .map_err(|e| {
                    let status = e.status;
                    http_adapter_step_error(e, status)
                })?;
            Err(Box::new(WaitForInput("awaiting human approval".to_string()))
                as Box<dyn std::error::Error + Send + Sync>)
        })
    });

    let ticket_send = ticket.clone();
    let mailer_send = Arc::clone(&mailer);
    let reader_store_send = Arc::clone(&reader_store);
    let send_reply: StepFunc = Box::new(move || {
        let ticket = ticket_send.clone();
        let mailer = Arc::clone(&mailer_send);
        let reader_store = Arc::clone(&reader_store_send);
        Box::pin(async move {
            let events = reader_store.load_events(&ticket.id)?;
            let approved_reply = step_completed_output(&events, 2)
                .ok_or("approval output missing")?;
            let subject = format!("Re: {}", ticket.subject);
            mailer
                .send_reply(&ticket.customer_id, &subject, &approved_reply)
                .await
                .map_err(|e| {
                    let status = e.status;
                    http_adapter_step_error(e, status)
                })?;
            Ok(approved_reply)
        })
    });

    let ticket_resolve = ticket.clone();
    let ticket_system_resolve = Arc::clone(&ticket_system);
    let update_ticket_system: StepFunc = Box::new(move || {
        let ticket_id = ticket_resolve.id.clone();
        let ticket_system = Arc::clone(&ticket_system_resolve);
        Box::pin(async move {
            ticket_system.mark_resolved(&ticket_id).await?;
            Ok("resolved".to_string())
        })
    });

    let complete: StepFunc = Box::new(|| Box::pin(async { Ok("done".to_string()) }));

    vec![
        ingest,
        classify,
        request_approval,
        send_reply,
        update_ticket_system,
        complete,
    ]
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use agentq::{DurableStore, Event, NonRetryable, Priority, Queue, SqliteStore, WorkflowEngine};

    use crate::approval::{MockApprovalSender, MockCustomerMailer};
    use crate::classifier::MockClassifier;
    use crate::domain::Ticket;
    use crate::ticket_system::InMemoryTicketSystem;
    use crate::workflow_def::ticket_workflow;

    use super::{
        approval_resume_output, build_step_bodies, http_adapter_step_error,
        is_non_retryable_status,
    };

    #[test]
    fn is_non_retryable_status_classifies_http_codes() {
        assert!(!is_non_retryable_status(None));
        assert!(!is_non_retryable_status(Some(429)));
        assert!(!is_non_retryable_status(Some(500)));
        assert!(!is_non_retryable_status(Some(503)));
        assert!(is_non_retryable_status(Some(400)));
        assert!(is_non_retryable_status(Some(404)));
        assert!(is_non_retryable_status(Some(422)));
    }

    #[derive(Debug)]
    struct SampleAdapterError(String);

    impl std::fmt::Display for SampleAdapterError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.0.fmt(f)
        }
    }

    impl std::error::Error for SampleAdapterError {}

    #[test]
    fn http_adapter_step_error_wraps_other_client_errors() {
        let retryable = http_adapter_step_error(SampleAdapterError("rate limited".into()), Some(429));
        assert!(retryable.downcast_ref::<NonRetryable>().is_none());

        let transient = http_adapter_step_error(SampleAdapterError("offline".into()), None);
        assert!(transient.downcast_ref::<NonRetryable>().is_none());

        let permanent =
            http_adapter_step_error(SampleAdapterError("bad request".into()), Some(400));
        assert!(permanent.downcast_ref::<NonRetryable>().is_some());
    }

    fn sample_ticket() -> Ticket {
        Ticket {
            id: "ticket-steps-test".into(),
            customer_id: "customer@example.com".into(),
            subject: "Need help".into(),
            body: "Just saying hello".into(),
        }
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

    fn step_completed(events: &[Event], step_index: usize) -> bool {
        events.iter().any(|e| {
            matches!(
                e,
                Event::StepCompleted {
                    step_index: si, ..
                } if *si == step_index
            )
        })
    }

    #[test]
    fn approval_resume_output_classifies_rejection_and_approval() {
        assert!(approval_resume_output("rejected".into()).is_err());
        assert!(approval_resume_output("REJECTED".into()).is_err());
        let err = approval_resume_output("rejected:not needed".into()).unwrap_err();
        assert_eq!(err.0, "not needed");
        assert_eq!(
            approval_resume_output("approved".into()).expect("approval"),
            "approved"
        );
        assert_eq!(
            approval_resume_output("garbage-token".into()).expect("approval"),
            "garbage-token"
        );
    }

    async fn workflow_waiting_fixture() -> (
        String,
        Arc<WorkflowEngine<SqliteStore>>,
        Arc<SqliteStore>,
        Ticket,
        Arc<InMemoryTicketSystem>,
    ) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static FIXTURE_ID: AtomicU64 = AtomicU64::new(0);
        let unique = FIXTURE_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "durable-agent-steps-test-{}-{}.db",
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

        let ticket = sample_ticket();
        let ticket_system = Arc::new(InMemoryTicketSystem::new());

        let bodies = build_step_bodies(
            ticket.clone(),
            Arc::new(MockClassifier),
            Arc::new(MockApprovalSender),
            Arc::new(MockCustomerMailer),
            Arc::clone(&ticket_system) as Arc<dyn crate::ticket_system::TicketSystem>,
            Arc::clone(&engine),
            Arc::clone(&reader_store),
        );

        engine
            .run(ticket_workflow(&ticket.id), bodies)
            .await
            .expect("run should pause at approval without error");

        (path_str, engine, reader_store, ticket, ticket_system)
    }

    #[tokio::test]
    async fn ticket_workflow_pauses_for_approval_then_completes() {
        let path = std::env::temp_dir().join(format!(
            "durable-agent-steps-test-{}.db",
            std::process::id()
        ));
        let path_str = path.to_str().expect("temp db path utf8");

        let store = SqliteStore::new(path_str).expect("engine store");
        let reader_store = Arc::new(SqliteStore::new(path_str).expect("reader store"));
        let queue = Queue::builder().start();
        let engine = Arc::new(WorkflowEngine::new(
            queue,
            store,
            "test-worker".to_string(),
            Duration::from_secs(60),
            Priority::High,
        ));

        let ticket = sample_ticket();
        let ticket_system = Arc::new(InMemoryTicketSystem::new());
        let ticket_system_assert = Arc::clone(&ticket_system);

        let bodies = build_step_bodies(
            ticket.clone(),
            Arc::new(MockClassifier),
            Arc::new(MockApprovalSender),
            Arc::new(MockCustomerMailer),
            ticket_system,
            Arc::clone(&engine),
            Arc::clone(&reader_store),
        );

        engine
            .run(ticket_workflow(&ticket.id), bodies)
            .await
            .expect("run should pause at approval without error");

        let events = reader_store
            .load_events(&ticket.id)
            .expect("load events after run");
        assert!(
            events
                .iter()
                .any(|e| matches!(e, Event::StepWaiting { step_index: 2, .. }))
        );
        assert!(!workflow_completed(&events));

        engine
            .resume(&ticket.id, 2, "approved".to_string())
            .await
            .expect("resume should complete workflow");

        let events = reader_store
            .load_events(&ticket.id)
            .expect("load events after resume");
        assert!(workflow_completed(&events));
        assert!(ticket_system_assert.is_resolved(&ticket.id));

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn ticket_workflow_rejection_fails_without_later_steps() {
        let (path_str, engine, reader_store, ticket, ticket_system) =
            workflow_waiting_fixture().await;

        let result = engine
            .resume(&ticket.id, 2, "rejected:not needed".to_string())
            .await;
        assert!(matches!(result, Err(agentq::EngineError::WorkflowFailed { .. })));

        let events = reader_store
            .load_events(&ticket.id)
            .expect("load events after rejection");
        assert!(workflow_failed(&events));
        assert!(!workflow_completed(&events));
        assert!(!step_completed(&events, 3));
        assert!(!step_completed(&events, 4));
        assert!(!step_completed(&events, 5));
        assert!(!ticket_system.is_resolved(&ticket.id));

        let _ = std::fs::remove_file(path_str);
    }

    #[tokio::test]
    async fn ticket_workflow_unrecognized_resume_input_treated_as_approval() {
        let (path_str, engine, reader_store, ticket, ticket_system) =
            workflow_waiting_fixture().await;

        engine
            .resume(&ticket.id, 2, "garbage-token".to_string())
            .await
            .expect("resume should complete workflow");

        let events = reader_store
            .load_events(&ticket.id)
            .expect("load events after resume");
        assert!(workflow_completed(&events));
        assert!(ticket_system.is_resolved(&ticket.id));
        assert!(step_completed(&events, 3));

        let _ = std::fs::remove_file(path_str);
    }

    #[tokio::test]
    async fn ticket_workflow_second_resume_after_approval_errors() {
        let (path_str, engine, reader_store, ticket, ticket_system) =
            workflow_waiting_fixture().await;

        engine
            .resume(&ticket.id, 2, "approved".to_string())
            .await
            .expect("first resume should complete workflow");
        assert!(workflow_completed(
            &reader_store.load_events(&ticket.id).expect("events")
        ));

        let second = engine
            .resume(&ticket.id, 2, "approved".to_string())
            .await;
        assert!(matches!(
            second,
            Err(agentq::EngineError::Store(agentq::StoreError::Backend(_)))
        ));
        assert!(ticket_system.is_resolved(&ticket.id));

        let _ = std::fs::remove_file(path_str);
    }

    #[tokio::test]
    async fn ticket_workflow_resume_after_completion_errors() {
        let (path_str, engine, reader_store, ticket, _) = workflow_waiting_fixture().await;

        engine
            .resume(&ticket.id, 2, "approved".to_string())
            .await
            .expect("resume should complete workflow");

        let after_complete = engine
            .resume(&ticket.id, 2, "rejected:nope".to_string())
            .await;
        assert!(matches!(
            after_complete,
            Err(agentq::EngineError::Store(agentq::StoreError::Backend(_)))
        ));
        assert!(workflow_completed(
            &reader_store.load_events(&ticket.id).expect("events")
        ));

        let _ = std::fs::remove_file(path_str);
    }
}
