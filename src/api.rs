use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use agentq::{
    DurableStore, EngineError, Event, Priority, Queue, SqliteStore, StoreError, WorkflowEngine,
    recover,
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
use crate::approval_correlations::ApprovalCorrelations;
use crate::classifier::{Classifier, MockClassifier, OpenAiClassifier};
use crate::domain::Ticket;
use crate::registry::WorkflowRegistry;
use crate::steps::build_step_bodies;
use crate::ticket_system::{InMemoryTicketSystem, TicketSystem};
use crate::tracing_sink::{NoopSink, RespanSink, TraceSink, spawn_workflow_trace_batch};
use crate::webhook::agentmail_webhook;
use crate::workflow_def::ticket_workflow;

const WORKER_ID: &str = "worker-1";

pub(crate) struct Metrics {
    workflows_created: AtomicU64,
    workflow_runs: AtomicU64,
    approvals: AtomicU64,
    rejections: AtomicU64,
    cancellations: AtomicU64,
    webhooks_processed: AtomicU64,
}

impl Metrics {
    fn new() -> Self {
        Self {
            workflows_created: AtomicU64::new(0),
            workflow_runs: AtomicU64::new(0),
            approvals: AtomicU64::new(0),
            rejections: AtomicU64::new(0),
            cancellations: AtomicU64::new(0),
            webhooks_processed: AtomicU64::new(0),
        }
    }

    fn inc_workflows_created(&self) {
        self.workflows_created.fetch_add(1, Ordering::Relaxed);
    }

    fn inc_workflow_runs(&self) {
        self.workflow_runs.fetch_add(1, Ordering::Relaxed);
    }

    fn inc_approvals(&self) {
        self.approvals.fetch_add(1, Ordering::Relaxed);
    }

    fn inc_rejections(&self) {
        self.rejections.fetch_add(1, Ordering::Relaxed);
    }

    fn inc_cancellations(&self) {
        self.cancellations.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn inc_webhooks_processed(&self) {
        self.webhooks_processed.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self, workflows_waiting: u64) -> MetricsView {
        MetricsView {
            workflows_created: self.workflows_created.load(Ordering::Relaxed),
            workflow_runs: self.workflow_runs.load(Ordering::Relaxed),
            approvals: self.approvals.load(Ordering::Relaxed),
            rejections: self.rejections.load(Ordering::Relaxed),
            cancellations: self.cancellations.load(Ordering::Relaxed),
            webhooks_processed: self.webhooks_processed.load(Ordering::Relaxed),
            workflows_waiting,
        }
    }
}

#[derive(serde::Serialize)]
struct MetricsView {
    workflows_created: u64,
    workflow_runs: u64,
    approvals: u64,
    rejections: u64,
    cancellations: u64,
    webhooks_processed: u64,
    workflows_waiting: u64,
}

#[derive(Clone)]
pub struct ApiState {
    pub engine: Arc<WorkflowEngine<SqliteStore>>,
    reader_store: Arc<SqliteStore>,
    registry: Arc<WorkflowRegistry>,
    pub correlations: Arc<ApprovalCorrelations>,
    pub(crate) webhook_secret: Option<String>,
    classifier: Arc<dyn Classifier>,
    approval_sender: Arc<dyn ApprovalSender>,
    mailer: Arc<dyn CustomerMailer>,
    ticket_system: Arc<dyn TicketSystem>,
    trace_sink: Arc<dyn TraceSink>,
    worker_id: String,
    pub(crate) metrics: Arc<Metrics>,
}

impl ApiState {
    pub fn new(db_path: &str) -> Self {
        let webhook_secret = std::env::var("AGENTMAIL_WEBHOOK_SECRET")
            .ok()
            .filter(|s| !s.is_empty());
        Self::build(db_path, webhook_secret)
    }

    #[cfg(test)]
    pub fn new_with_webhook_secret(db_path: &str, webhook_secret: &str) -> Self {
        Self::build(db_path, Some(webhook_secret.to_string()))
    }

    #[cfg(test)]
    pub fn new_with_webhook_secret_and_trace_sink(
        db_path: &str,
        webhook_secret: &str,
        trace_sink: Arc<dyn TraceSink>,
    ) -> Self {
        let mut state = Self::build(db_path, Some(webhook_secret.to_string()));
        state.trace_sink = trace_sink;
        state
    }

    #[cfg(test)]
    pub fn new_with_trace_sink(db_path: &str, trace_sink: Arc<dyn TraceSink>) -> Self {
        let mut state = Self::build(db_path, None);
        state.trace_sink = trace_sink;
        state
    }

    #[cfg(test)]
    pub fn new_with_classifier_and_trace_sink(
        db_path: &str,
        classifier: Arc<dyn Classifier>,
        trace_sink: Arc<dyn TraceSink>,
    ) -> Self {
        let mut state = Self::build(db_path, None);
        state.classifier = classifier;
        state.trace_sink = trace_sink;
        state
    }

    #[cfg(test)]
    pub fn new_with_ticket_system_and_trace_sink(
        db_path: &str,
        ticket_system: Arc<dyn TicketSystem>,
        trace_sink: Arc<dyn TraceSink>,
    ) -> Self {
        let mut state = Self::build(db_path, None);
        state.ticket_system = ticket_system;
        state.trace_sink = trace_sink;
        state
    }

    fn build(db_path: &str, webhook_secret: Option<String>) -> Self {
        let engine_store = SqliteStore::new(db_path).expect("engine sqlite store");
        let reader_store = Arc::new(SqliteStore::new(db_path).expect("reader sqlite store"));
        let queue = Queue::builder().start();
        let worker_id = WORKER_ID.to_string();
        let engine = Arc::new(WorkflowEngine::new(
            queue,
            engine_store,
            worker_id.clone(),
            Duration::from_secs(30),
            Priority::Medium,
        ));
        let registry = Arc::new(WorkflowRegistry::new(db_path).expect("workflow registry"));
        let correlations =
            Arc::new(ApprovalCorrelations::new(db_path).expect("approval correlations"));

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

        let trace_sink: Arc<dyn TraceSink> = match std::env::var("RESPAN_API_KEY")
            .ok()
            .filter(|s| !s.is_empty())
        {
            Some(key) => Arc::new(RespanSink::new(key)),
            None => Arc::new(NoopSink),
        };

        Self {
            engine,
            reader_store,
            registry,
            correlations,
            webhook_secret,
            classifier,
            approval_sender,
            mailer,
            ticket_system,
            trace_sink,
            worker_id,
            metrics: Arc::new(Metrics::new()),
        }
    }

    fn count_workflows_waiting(&self) -> Result<u64, StatusCode> {
        let ids = self.registry.list().map_err(|e| {
            tracing::error!(error = %e, "registry list failed for metrics");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
        let mut waiting = 0u64;
        for id in ids {
            let events = self.reader_store.load_events(&id).map_err(|e| {
                tracing::error!(workflow_id = %id, error = %e, "load_events failed for metrics");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
            let (status, _) = workflow_status_from_events(&events);
            if status == "waiting" {
                waiting += 1;
            }
        }
        Ok(waiting)
    }

    pub async fn recover_pending_workflows(&self) -> Result<usize, EngineError> {
        let ids = self
            .registry
            .list()
            .map_err(|e| EngineError::Store(StoreError::Backend(e.to_string())))?;

        let mut recovery_trace_targets: Vec<String> = Vec::new();

        for id in ids {
            let ticket = match self.registry.get(&id) {
                Ok(Some(ticket)) => ticket,
                Ok(None) => {
                    tracing::error!(
                        workflow_id = %id,
                        "startup recovery: workflow not found in registry"
                    );
                    continue;
                }
                Err(e) => {
                    tracing::error!(
                        workflow_id = %id,
                        error = %e,
                        "startup recovery: registry get failed"
                    );
                    continue;
                }
            };

            let events = match self.reader_store.load_events(&id) {
                Ok(events) => events,
                Err(e) => {
                    tracing::error!(
                        workflow_id = %id,
                        error = %e,
                        "startup recovery: load_events failed"
                    );
                    continue;
                }
            };

            let (status, _) = workflow_status_from_events(&events);
            if status == "completed" || status == "failed" {
                continue;
            }

            let workflow = ticket_workflow(&id);
            let bodies = build_step_bodies(
                ticket,
                Arc::clone(&self.classifier),
                Arc::clone(&self.approval_sender),
                Arc::clone(&self.mailer),
                Arc::clone(&self.ticket_system),
                Arc::clone(&self.engine),
                Arc::clone(&self.reader_store),
                Arc::clone(&self.correlations),
            );

            if let Err(e) = self.engine.register_workflow(workflow, bodies) {
                tracing::error!(
                    workflow_id = %id,
                    error = %e,
                    "startup recovery: register_workflow failed"
                );
                continue;
            }
            recovery_trace_targets.push(id);
        }

        let count = recover(&self.engine).await?;
        for workflow_id in recovery_trace_targets {
            spawn_workflow_trace_batch(
                Arc::clone(&self.reader_store),
                Arc::clone(&self.trace_sink),
                self.worker_id.clone(),
                workflow_id,
            );
        }
        Ok(count)
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
            Event::WorkflowCancelled { .. } => {
                status = "cancelled".to_string();
                waiting_step = None;
            }
            Event::StepWaiting { step_index, .. } => {
                status = "waiting".to_string();
                waiting_step = Some(*step_index);
            }
            Event::StepResumed { step_index, .. }
            | Event::StepCompleted { step_index, .. }
            | Event::WorkerRecovered { step_index, .. }
                if waiting_step == Some(*step_index) =>
            {
                waiting_step = None;
                if status == "waiting" {
                    status = "running".to_string();
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

#[cfg(feature = "dev-tools")]
pub(crate) fn dev_kill_allowed() -> bool {
    matches!(
        std::env::var("DURABLE_AGENT_ALLOW_DEV_KILL"),
        Ok(value) if value == "1"
    )
}

#[cfg(feature = "dev-tools")]
async fn dev_kill_worker() -> StatusCode {
    if !dev_kill_allowed() {
        return StatusCode::FORBIDDEN;
    }
    let pid = std::process::id();
    let result = unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) };
    if result != 0 {
        return StatusCode::INTERNAL_SERVER_ERROR;
    }
    StatusCode::OK
}

pub fn router() -> Router<ApiState> {
    let router = Router::new()
        .route("/workflows", post(create_workflow).get(list_workflows))
        .route("/workflows/{id}", get(get_workflow))
        .route("/workflows/{id}/run", post(run_workflow))
        .route("/workflows/{id}/events", get(workflow_events))
        .route("/workflows/{id}/approve", post(approve_workflow))
        .route("/workflows/{id}/reject", post(reject_workflow))
        .route("/workflows/{id}/cancel", post(cancel_workflow))
        .route("/webhooks/agentmail", post(agentmail_webhook))
        .route("/metrics", get(get_metrics));
    #[cfg(feature = "dev-tools")]
    {
        router.route("/dev/kill", post(dev_kill_worker))
    }
    #[cfg(not(feature = "dev-tools"))]
    {
        router
    }
}

async fn get_metrics(State(state): State<ApiState>) -> Result<Json<MetricsView>, StatusCode> {
    let workflows_waiting = state.count_workflows_waiting()?;
    Ok(Json(state.metrics.snapshot(workflows_waiting)))
}

async fn create_workflow(
    State(state): State<ApiState>,
    Json(ticket): Json<Ticket>,
) -> Result<(StatusCode, Json<Ticket>), StatusCode> {
    match state.registry.insert(&ticket) {
        Ok(()) => {
            state.metrics.inc_workflows_created();
            Ok((StatusCode::CREATED, Json(ticket)))
        }
        Err(e) if is_unique_violation(&e) => Err(StatusCode::CONFLICT),
        Err(e) => {
            tracing::error!(workflow_id = %ticket.id, error = %e, "registry insert failed");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn list_workflows(State(state): State<ApiState>) -> Result<Json<Vec<String>>, StatusCode> {
    let ids = state.registry.list().map_err(|e| {
        tracing::error!(error = %e, "registry list failed");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    Ok(Json(ids))
}

async fn get_workflow(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<WorkflowView>, StatusCode> {
    let ticket = state.registry.get(&id).map_err(|e| {
        tracing::error!(workflow_id = %id, error = %e, "registry get failed");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    if ticket.is_none() {
        return Err(StatusCode::NOT_FOUND);
    }

    let events = state.reader_store.load_events(&id).map_err(|e| {
        tracing::error!(workflow_id = %id, error = %e, "load_events failed");
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
        tracing::error!(workflow_id = %id, error = %e, "registry get failed");
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
        Arc::clone(&state.correlations),
    );

    let engine = Arc::clone(&state.engine);
    let reader_store = Arc::clone(&state.reader_store);
    let trace_sink = Arc::clone(&state.trace_sink);
    let worker_id = state.worker_id.clone();
    let workflow_id = id.clone();
    tokio::spawn(async move {
        if let Err(e) = engine.run(workflow, bodies).await {
            tracing::error!(workflow_id = %workflow_id, error = %e, "workflow run failed");
        }
        spawn_workflow_trace_batch(reader_store, trace_sink, worker_id, workflow_id);
    });

    state.metrics.inc_workflow_runs();
    Ok(StatusCode::ACCEPTED)
}

async fn workflow_events(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<Vec<Event>>, StatusCode> {
    let events = state.reader_store.load_events(&id).map_err(|e| {
        tracing::error!(workflow_id = %id, error = %e, "load_events failed");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    Ok(Json(events))
}

fn waiting_step_index(reader_store: &SqliteStore, id: &str) -> Result<usize, StatusCode> {
    let events = reader_store.load_events(id).map_err(|e| {
        tracing::error!(workflow_id = %id, error = %e, "load_events failed");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let (status, waiting_step) = workflow_status_from_events(&events);
    if status != "waiting" {
        return Err(StatusCode::NOT_FOUND);
    }
    Ok(waiting_step.expect("waiting status implies waiting_step"))
}

fn spawn_resume(
    engine: Arc<WorkflowEngine<SqliteStore>>,
    reader_store: Arc<SqliteStore>,
    trace_sink: Arc<dyn TraceSink>,
    worker_id: String,
    id: String,
    step_index: usize,
    input: String,
) {
    tokio::spawn(async move {
        if let Err(e) = engine.resume(&id, step_index, input).await {
            tracing::error!(
                workflow_id = %id,
                step_index,
                error = %e,
                "workflow resume failed"
            );
        }
        spawn_workflow_trace_batch(reader_store, trace_sink, worker_id, id);
    });
}

async fn approve_workflow(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    body: Bytes,
) -> Result<StatusCode, StatusCode> {
    let step_index = waiting_step_index(state.reader_store.as_ref(), &id)?;
    let input = String::from_utf8(body.to_vec()).map_err(|_| StatusCode::BAD_REQUEST)?;
    spawn_resume(
        Arc::clone(&state.engine),
        Arc::clone(&state.reader_store),
        Arc::clone(&state.trace_sink),
        state.worker_id.clone(),
        id,
        step_index,
        input,
    );
    state.metrics.inc_approvals();
    Ok(StatusCode::ACCEPTED)
}

async fn reject_workflow(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    body: Bytes,
) -> Result<StatusCode, StatusCode> {
    let step_index = waiting_step_index(state.reader_store.as_ref(), &id)?;
    let reason = String::from_utf8(body.to_vec()).map_err(|_| StatusCode::BAD_REQUEST)?;
    let input = if reason.is_empty() {
        "rejected".to_string()
    } else {
        format!("rejected:{reason}")
    };
    spawn_resume(
        Arc::clone(&state.engine),
        Arc::clone(&state.reader_store),
        Arc::clone(&state.trace_sink),
        state.worker_id.clone(),
        id,
        step_index,
        input,
    );
    state.metrics.inc_rejections();
    Ok(StatusCode::ACCEPTED)
}

async fn cancel_workflow(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    body: Bytes,
) -> Result<StatusCode, StatusCode> {
    let ticket = state.registry.get(&id).map_err(|e| {
        tracing::error!(workflow_id = %id, error = %e, "registry get failed");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    if ticket.is_none() {
        return Err(StatusCode::NOT_FOUND);
    }

    let reason = String::from_utf8(body.to_vec()).map_err(|_| StatusCode::BAD_REQUEST)?;
    let reason = if reason.is_empty() {
        "cancelled".to_string()
    } else {
        reason
    };

    if let Err(e) = state.engine.cancel(&id, reason).await {
        tracing::error!(workflow_id = %id, error = %e, "workflow cancel failed");
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    state.metrics.inc_cancellations();
    Ok(StatusCode::OK)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::Ticket;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn recovery_test_db_path(label: &str) -> String {
        std::env::temp_dir()
            .join(format!(
                "durable-agent-recover-test-{}-{}.db",
                std::process::id(),
                label
            ))
            .to_str()
            .expect("temp db path utf8")
            .to_string()
    }

    fn sample_ticket(id: &str) -> Ticket {
        Ticket {
            id: id.to_string(),
            customer_id: "customer@example.com".to_string(),
            subject: "Need help".to_string(),
            body: "Just saying hello".to_string(),
        }
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
            rusqlite::params![workflow_id, 3_i64, expires_at_secs, expires_at_nanos],
        )
        .expect("seed expired lease on SendReply step");
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

    fn seed_waiting_at_approval(path: &str, workflow_id: &str, classification_json: &str) {
        let store = SqliteStore::new(path).expect("seed store");
        store
            .append_event(&Event::WorkflowStarted {
                workflow_id: workflow_id.to_string(),
            })
            .expect("WorkflowStarted");
        for (step_index, output) in [
            (0usize, workflow_id.to_string()),
            (1, classification_json.to_string()),
        ] {
            store
                .append_event(&Event::StepCompleted {
                    workflow_id: workflow_id.to_string(),
                    step_index,
                    output,
                })
                .expect("StepCompleted");
        }
        store
            .append_event(&Event::StepWaiting {
                workflow_id: workflow_id.to_string(),
                step_index: 2,
                reason: "awaiting human approval".into(),
            })
            .expect("StepWaiting");
    }

    #[tokio::test]
    async fn recover_pending_workflows_after_simulated_crash() {
        use crate::classifier::{Classifier, MockClassifier};

        let path = recovery_test_db_path("crash-complete");
        let _ = std::fs::remove_file(&path);

        let workflow_id = "wf-recover-startup";
        let ticket = sample_ticket(workflow_id);
        let classification = MockClassifier
            .classify(&ticket)
            .await
            .expect("mock classify");
        let classification_json =
            serde_json::to_string(&classification).expect("classification json");

        {
            let registry = WorkflowRegistry::new(&path).expect("registry");
            registry.insert(&ticket).expect("insert ticket");
            seed_crash_mid_send_reply(&path, workflow_id, &classification_json);
        }

        let state = ApiState::new(&path);
        let recovered = state
            .recover_pending_workflows()
            .await
            .expect("recover_pending_workflows");
        assert!(recovered >= 1, "expected at least one re-admitted step");

        let reader = SqliteStore::new(&path).expect("poll store");
        for _ in 0..200 {
            let events = reader
                .load_events(workflow_id)
                .expect("load_events while polling");
            if workflow_completed(&events) {
                let _ = std::fs::remove_file(&path);
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        let events = reader.load_events(workflow_id).expect("final events");
        panic!("workflow did not reach WorkflowCompleted after recovery; last events: {events:?}");
    }

    #[tokio::test]
    async fn recover_pending_workflows_after_restart_then_reject() {
        use crate::classifier::{Classifier, MockClassifier};

        let path = recovery_test_db_path("restart-reject");
        let _ = std::fs::remove_file(&path);

        let workflow_id = "wf-recover-reject";
        let ticket = sample_ticket(workflow_id);
        let classification = MockClassifier
            .classify(&ticket)
            .await
            .expect("mock classify");
        let classification_json =
            serde_json::to_string(&classification).expect("classification json");

        {
            let registry = WorkflowRegistry::new(&path).expect("registry");
            registry.insert(&ticket).expect("insert ticket");
            seed_waiting_at_approval(&path, workflow_id, &classification_json);
        }

        let state = ApiState::new(&path);
        state
            .recover_pending_workflows()
            .await
            .expect("recover_pending_workflows");

        let step_index =
            waiting_step_index(state.reader_store.as_ref(), workflow_id).expect("waiting step");
        assert_eq!(step_index, 2);

        spawn_resume(
            Arc::clone(&state.engine),
            Arc::clone(&state.reader_store),
            Arc::clone(&state.trace_sink),
            state.worker_id.clone(),
            workflow_id.to_string(),
            step_index,
            "rejected:post-restart".to_string(),
        );

        let reader = SqliteStore::new(&path).expect("poll store");
        for _ in 0..200 {
            let events = reader
                .load_events(workflow_id)
                .expect("load_events while polling");
            if workflow_failed(&events) {
                assert!(!workflow_completed(&events));
                let _ = std::fs::remove_file(&path);
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        let events = reader.load_events(workflow_id).expect("final events");
        panic!(
            "workflow did not reach WorkflowFailed after recovery rejection; last events: {events:?}"
        );
    }

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

    #[test]
    fn workflow_status_cancelled_clears_waiting() {
        let events = vec![
            Event::StepWaiting {
                workflow_id: "w".into(),
                step_index: 2,
                reason: "approval".into(),
            },
            Event::WorkflowCancelled {
                workflow_id: "w".into(),
                reason: "stop".into(),
            },
        ];
        let (status, waiting) = workflow_status_from_events(&events);
        assert_eq!(status, "cancelled");
        assert!(waiting.is_none());
    }

    #[tokio::test]
    async fn cancel_running_workflow_via_http_stops_before_later_step() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tower::ServiceExt;

        let path = recovery_test_db_path("cancel-running-http");
        let _ = std::fs::remove_file(&path);
        let state = ApiState::new(&path);
        let workflow_id = "wf-cancel-running";
        let ticket = sample_ticket(workflow_id);
        state.registry.insert(&ticket).expect("insert");

        let step1_ran = Arc::new(AtomicUsize::new(0));
        let step1_ran_clone = step1_ran.clone();
        let step0_done = Arc::new(tokio::sync::Notify::new());
        let step0_done_clone = step0_done.clone();

        let no_retry = agentq::RetryPolicy {
            max_attempts: 1,
            backoff: agentq::Backoff::Fixed(Duration::from_millis(1)),
        };
        let workflow = agentq::Workflow {
            id: workflow_id.to_string(),
            steps: vec![
                agentq::StepDef {
                    name: "first".into(),
                    retry_policy: no_retry.clone(),
                    timeout: None,
                },
                agentq::StepDef {
                    name: "second".into(),
                    retry_policy: no_retry,
                    timeout: None,
                },
            ],
        };

        let step0: agentq::StepFunc = Box::new(move || {
            let notify = step0_done_clone.clone();
            Box::pin(async move {
                notify.notify_one();
                tokio::task::yield_now().await;
                Ok("first".to_string())
            })
        });
        let step1: agentq::StepFunc = Box::new(move || {
            let counter = step1_ran_clone.clone();
            Box::pin(async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok("second".to_string())
            })
        });

        let engine = Arc::clone(&state.engine);
        let run_handle = tokio::spawn(async move {
            engine.run(workflow, vec![step0, step1]).await.expect("run");
        });

        step0_done.notified().await;

        let app = router().with_state(state);
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/workflows/{workflow_id}/cancel"))
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("oneshot");
        assert_eq!(response.status(), StatusCode::OK);

        run_handle.await.expect("run task");

        assert_eq!(
            step1_ran.load(Ordering::SeqCst),
            0,
            "step after cancellation must not run"
        );

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/workflows/{workflow_id}"))
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
        assert_eq!(view["status"], "cancelled");

        let _ = std::fs::remove_file(&path);
    }

    #[cfg(feature = "dev-tools")]
    #[test]
    fn dev_kill_allowed_requires_exact_env_value() {
        unsafe { std::env::remove_var("DURABLE_AGENT_ALLOW_DEV_KILL") };
        assert!(!dev_kill_allowed());

        unsafe { std::env::set_var("DURABLE_AGENT_ALLOW_DEV_KILL", "true") };
        assert!(!dev_kill_allowed());

        unsafe { std::env::set_var("DURABLE_AGENT_ALLOW_DEV_KILL", "1") };
        assert!(dev_kill_allowed());

        unsafe { std::env::remove_var("DURABLE_AGENT_ALLOW_DEV_KILL") };
    }

    #[tokio::test]
    async fn workflow_completes_without_respan_api_key() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        unsafe { std::env::remove_var("RESPAN_API_KEY") };
        assert!(std::env::var("RESPAN_API_KEY").is_err());

        let path = recovery_test_db_path("no-respan");
        let _ = std::fs::remove_file(&path);
        let app = router().with_state(ApiState::new(&path));
        let workflow_id = "wf-no-respan";
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

        for _ in 0..200 {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/workflows/{workflow_id}"))
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
            if view["status"] == "waiting" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

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

        for _ in 0..200 {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/workflows/{workflow_id}"))
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
            if view["status"] == "completed" {
                let _ = std::fs::remove_file(&path);
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("workflow did not complete without RESPAN_API_KEY");
    }

    #[tokio::test]
    async fn respan_failure_does_not_block_workflow() {
        use crate::tracing_sink::RespanSink;
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        let path = recovery_test_db_path("respan-fail");
        let _ = std::fs::remove_file(&path);
        let sink = Arc::new(RespanSink::with_ingest_url(
            "test-key".into(),
            "http://127.0.0.1:1/".into(),
        ));
        let app = router().with_state(ApiState::new_with_trace_sink(&path, sink));
        let workflow_id = "wf-respan-fail";
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

        for _ in 0..200 {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/workflows/{workflow_id}"))
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
            if view["status"] == "waiting" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

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

        for _ in 0..200 {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/workflows/{workflow_id}"))
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
            if view["status"] == "completed" {
                let _ = std::fs::remove_file(&path);
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("workflow did not complete when respan ingest is unreachable");
    }

    #[cfg(feature = "dev-tools")]
    #[tokio::test]
    async fn dev_kill_returns_forbidden_without_env_and_server_stays_alive() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        unsafe { std::env::remove_var("DURABLE_AGENT_ALLOW_DEV_KILL") };

        let path = recovery_test_db_path("dev-kill-forbidden");
        let _ = std::fs::remove_file(&path);
        let state = ApiState::new(&path);
        let app = router().with_state(state);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/dev/kill")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("oneshot");
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/workflows")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("oneshot");
        assert_eq!(response.status(), StatusCode::OK);

        let _ = std::fs::remove_file(&path);
    }
}
