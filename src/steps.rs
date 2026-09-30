use std::sync::Arc;

use agentq::{DurableStore, Event, StepFunc, WaitForInput, WorkflowEngine};

use crate::approval::{ApprovalSender, CustomerMailer};
use crate::classifier::Classifier;
use crate::domain::{Classification, Ticket};
use crate::ticket_system::TicketSystem;

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
            let classification = classifier.classify(&ticket).await?;
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
                return Ok(input);
            }

            let events = reader_store.load_events(&ticket.id)?;
            let classification_json = step_completed_output(&events, 1)
                .ok_or("classification output missing")?;
            let classification: Classification = serde_json::from_str(&classification_json)?;
            approval_sender
                .request_approval(&ticket, &classification.draft_reply)
                .await?;
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
                .await?;
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

    use agentq::{DurableStore, Event, Priority, Queue, SqliteStore, WorkflowEngine};

    use crate::approval::{MockApprovalSender, MockCustomerMailer};
    use crate::classifier::MockClassifier;
    use crate::domain::Ticket;
    use crate::ticket_system::InMemoryTicketSystem;
    use crate::workflow_def::ticket_workflow;

    use super::build_step_bodies;

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
}
