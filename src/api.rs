use std::sync::Arc;
use std::time::Duration;

use agentq::{
    DurableStore, Event, Priority, Queue, SqliteStore, WorkflowEngine,
};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
};

use crate::approval::{
    AgentMailSender, ApprovalSender, CustomerMailer, MockApprovalSender, MockCustomerMailer,
};
use crate::classifier::{Classifier, MockClassifier, OpenAiClassifier};
use crate::domain::Ticket;
use crate::registry::WorkflowRegistry;
use crate::steps::build_step_bodies;
use crate::ticket_system::{InMemoryTicketSystem, TicketSystem};
use crate::workflow_def::ticket_workflow;

#[derive(Clone)]
pub struct ApiState {
    engine: Arc<WorkflowEngine<SqliteStore>>,
    reader_store: Arc<SqliteStore>,
    registry: Arc<WorkflowRegistry>,
    classifier: Arc<dyn Classifier>,
    approval_sender: Arc<dyn ApprovalSender>,
    mailer: Arc<dyn CustomerMailer>,
    ticket_system: Arc<dyn TicketSystem>,
}

impl ApiState {
    pub fn new(db_path: &str) -> Self {
        let engine_store = SqliteStore::new(db_path).expect("engine sqlite store");
        let reader_store = Arc::new(
            SqliteStore::new(db_path).expect("reader sqlite store"),
        );
        let queue = Queue::builder().start();
        let engine = Arc::new(WorkflowEngine::new(
            queue,
            engine_store,
            "worker-1".to_string(),
            Duration::from_secs(30),
            Priority::Medium,
        ));
        let registry = Arc::new(
            WorkflowRegistry::new(db_path).expect("workflow registry"),
        );

        let classifier: Arc<dyn Classifier> = if std::env::var("OPENAI_API_KEY").is_ok() {
            Arc::new(OpenAiClassifier::new())
        } else {
            Arc::new(MockClassifier)
        };

        let (approval_sender, mailer): (Arc<dyn ApprovalSender>, Arc<dyn CustomerMailer>) =
            if std::env::var("AGENTMAIL_API_KEY").is_ok()
                && std::env::var("AGENTMAIL_FROM_ADDRESS").is_ok()
                && std::env::var("APPROVER_EMAIL").is_ok()
            {
                let sender = AgentMailSender::new(
                    std::env::var("AGENTMAIL_API_KEY").expect("AGENTMAIL_API_KEY"),
                    std::env::var("AGENTMAIL_FROM_ADDRESS").expect("AGENTMAIL_FROM_ADDRESS"),
                    std::env::var("APPROVER_EMAIL").expect("APPROVER_EMAIL"),
                );
                (
                    Arc::new(sender.clone()) as Arc<dyn ApprovalSender>,
                    Arc::new(sender) as Arc<dyn CustomerMailer>,
                )
            } else {
                (
                    Arc::new(MockApprovalSender) as Arc<dyn ApprovalSender>,
                    Arc::new(MockCustomerMailer) as Arc<dyn CustomerMailer>,
                )
            };

        let ticket_system: Arc<dyn TicketSystem> = Arc::new(InMemoryTicketSystem::new());

        Self {
            engine,
            reader_store,
            registry,
            classifier,
            approval_sender,
            mailer,
            ticket_system,
        }
    }
}

#[derive(serde::Serialize)]
struct WorkflowView {
    id: String,
    status: String,
    waiting_step: Option<usize>,
}

fn workflow_status_from_events(events: &[Event]) -> (String, Option<usize>) {
    if events.is_empty() {
        return ("pending".to_string(), None);
    }

    let mut status = "running".to_string();
    let mut waiting_step: Option<usize> = None;

    for event in events {
        match event {
            Event::WorkflowCompleted { .. } => {
                status = "completed".to_string();
                waiting_step = None;
            }
            Event::WorkflowFailed { .. } => {
                status = "failed".to_string();
                waiting_step = None;
            }
            Event::StepWaiting { step_index, .. } => {
                status = "waiting".to_string();
                waiting_step = Some(*step_index);
            }
            Event::StepResumed { step_index, .. }
            | Event::StepCompleted { step_index, .. }
            | Event::WorkerRecovered { step_index, .. } => {
                if waiting_step == Some(*step_index) {
                    waiting_step = None;
                    if status == "waiting" {
                        status = "running".to_string();
                    }
                }
            }
            _ => {}
        }
    }

    (status, waiting_step)
}

fn is_unique_violation(err: &rusqlite::Error) -> bool {
    matches!(
        err,
        rusqlite::Error::SqliteFailure(e, _)
            if e.code == rusqlite::ErrorCode::ConstraintViolation
    )
}

pub fn router() -> Router<ApiState> {
    Router::new()
        .route("/workflows", post(create_workflow).get(list_workflows))
        .route("/workflows/{id}", get(get_workflow))
        .route("/workflows/{id}/run", post(run_workflow))
        .route("/workflows/{id}/events", get(workflow_events))
        .route("/workflows/{id}/approve", post(approve_workflow))
}

async fn create_workflow(
    State(state): State<ApiState>,
    Json(ticket): Json<Ticket>,
) -> Result<(StatusCode, Json<Ticket>), StatusCode> {
    match state.registry.insert(&ticket) {
        Ok(()) => Ok((StatusCode::CREATED, Json(ticket))),
        Err(e) if is_unique_violation(&e) => Err(StatusCode::CONFLICT),
        Err(e) => {
            eprintln!("registry insert failed: {e}");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn list_workflows(
    State(state): State<ApiState>,
) -> Result<Json<Vec<String>>, StatusCode> {
    let ids = state.registry.list().map_err(|e| {
        eprintln!("registry list failed: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    Ok(Json(ids))
}

async fn get_workflow(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<WorkflowView>, StatusCode> {
    let ticket = state.registry.get(&id).map_err(|e| {
        eprintln!("registry get failed: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    if ticket.is_none() {
        return Err(StatusCode::NOT_FOUND);
    }

    let events = state.reader_store.load_events(&id).map_err(|e| {
        eprintln!("load_events failed: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let (status, waiting_step) = workflow_status_from_events(&events);

    Ok(Json(WorkflowView {
        id,
        status,
        waiting_step,
    }))
}

async fn run_workflow(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<StatusCode, StatusCode> {
    let ticket = state.registry.get(&id).map_err(|e| {
        eprintln!("registry get failed: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let ticket = ticket.ok_or(StatusCode::NOT_FOUND)?;

    let workflow = ticket_workflow(&id);
    let bodies = build_step_bodies(
        ticket,
        Arc::clone(&state.classifier),
        Arc::clone(&state.approval_sender),
        Arc::clone(&state.mailer),
        Arc::clone(&state.ticket_system),
        Arc::clone(&state.engine),
        Arc::clone(&state.reader_store),
    );

    let engine = Arc::clone(&state.engine);
    tokio::spawn(async move {
        if let Err(e) = engine.run(workflow, bodies).await {
            eprintln!("workflow run failed: {e}");
        }
    });

    Ok(StatusCode::ACCEPTED)
}

async fn workflow_events(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<Vec<Event>>, StatusCode> {
    let events = state.reader_store.load_events(&id).map_err(|e| {
        eprintln!("load_events failed: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    Ok(Json(events))
}

async fn approve_workflow(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    body: Bytes,
) -> Result<StatusCode, StatusCode> {
    let events = state.reader_store.load_events(&id).map_err(|e| {
        eprintln!("load_events failed: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let (status, waiting_step) = workflow_status_from_events(&events);
    if status != "waiting" {
        return Err(StatusCode::NOT_FOUND);
    }
    let step_index = waiting_step.expect("waiting status implies waiting_step");

    let input = String::from_utf8(body.to_vec()).map_err(|_| StatusCode::BAD_REQUEST)?;

    let engine = Arc::clone(&state.engine);
    tokio::spawn(async move {
        if let Err(e) = engine.resume(&id, step_index, input).await {
            eprintln!("workflow resume failed: {e}");
        }
    });

    Ok(StatusCode::ACCEPTED)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workflow_status_pending_without_events() {
        let (status, waiting) = workflow_status_from_events(&[]);
        assert_eq!(status, "pending");
        assert!(waiting.is_none());
    }

    #[test]
    fn workflow_status_waiting_until_resumed() {
        let events = vec![
            Event::WorkflowStarted {
                workflow_id: "w".into(),
            },
            Event::StepWaiting {
                workflow_id: "w".into(),
                step_index: 2,
                reason: "approval".into(),
            },
        ];
        let (status, waiting) = workflow_status_from_events(&events);
        assert_eq!(status, "waiting");
        assert_eq!(waiting, Some(2));

        let events = vec![
            Event::WorkflowStarted {
                workflow_id: "w".into(),
            },
            Event::StepWaiting {
                workflow_id: "w".into(),
                step_index: 2,
                reason: "approval".into(),
            },
            Event::StepResumed {
                workflow_id: "w".into(),
                step_index: 2,
            },
        ];
        let (status, waiting) = workflow_status_from_events(&events);
        assert_eq!(status, "running");
        assert!(waiting.is_none());
    }

    #[test]
    fn workflow_status_completed_wins() {
        let events = vec![
            Event::StepWaiting {
                workflow_id: "w".into(),
                step_index: 2,
                reason: "approval".into(),
            },
            Event::StepResumed {
                workflow_id: "w".into(),
                step_index: 2,
            },
            Event::WorkflowCompleted {
                workflow_id: "w".into(),
            },
        ];
        let (status, _) = workflow_status_from_events(&events);
        assert_eq!(status, "completed");
    }
}
