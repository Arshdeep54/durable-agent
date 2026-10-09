use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use agentq::{
    DurableStore, EngineError, Event, Priority, Queue, SqliteStore, StoreError, WorkflowEngine,
    recover,
};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, Request, State},
    http::StatusCode,
    middleware::{self, Next},
    response::Response,
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
use crate::ticket_system::{SqliteTicketSystem, TicketSystem};
use crate::tracing_sink::{NoopSink, RespanSink, TraceSink, spawn_workflow_trace_batch};
use crate::webhook::agentmail_webhook;
use crate::workflow_def::ticket_workflow;

const WORKER_ID: &str = "worker-1";

/// Counters persisted in SQLite so the dashboard survives restarts.
pub(crate) struct Metrics {
    conn: Mutex<rusqlite::Connection>,
}

impl Metrics {
    fn new(db_path: &str) -> rusqlite::Result<Self> {
        let conn = rusqlite::Connection::open(db_path)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS metrics (name TEXT PRIMARY KEY, value INTEGER NOT NULL);",
        )?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    fn inc(&self, name: &str) {
        let conn = self.conn.lock().expect("metrics connection");
        if let Err(e) = conn.execute(
            "INSERT INTO metrics (name, value) VALUES (?1, 1)
             ON CONFLICT(name) DO UPDATE SET value = value + 1",
            [name],
        ) {
            tracing::warn!(metric = name, error = %e, "metrics increment failed");
        }
    }

    fn inc_workflows_created(&self) {
        self.inc("workflows_created");
    }

    fn inc_workflow_runs(&self) {
        self.inc("workflow_runs");
    }

    fn inc_approvals(&self) {
        self.inc("approvals");
    }

    fn inc_rejections(&self) {
        self.inc("rejections");
    }

    fn inc_cancellations(&self) {
        self.inc("cancellations");
    }

    pub(crate) fn inc_webhooks_processed(&self) {
        self.inc("webhooks_processed");
    }

    fn snapshot(&self, workflows_waiting: u64) -> rusqlite::Result<MetricsView> {
        let conn = self.conn.lock().expect("metrics connection");
        let get = |name: &str| -> rusqlite::Result<u64> {
            conn.query_row("SELECT value FROM metrics WHERE name = ?1", [name], |r| {
                r.get::<_, i64>(0)
            })
            .map(|v| v as u64)
            .or_else(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => Ok(0),
                e => Err(e),
            })
        };
        Ok(MetricsView {
            workflows_created: get("workflows_created")?,
            workflow_runs: get("workflow_runs")?,
            approvals: get("approvals")?,
            rejections: get("rejections")?,
            cancellations: get("cancellations")?,
            webhooks_processed: get("webhooks_processed")?,
            workflows_waiting,
        })
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
    pub(crate) api_key: Option<String>,
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
    pub fn new_with_api_key(db_path: &str, api_key: &str) -> Self {
        let mut state = Self::build(db_path, None);
        state.api_key = Some(api_key.to_string());
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
        let api_key = api_key_from_env();
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
            tracing::info!("OPENAI_API_KEY not set — using MockClassifier");
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
                tracing::info!(
                    "AGENTMAIL_API_KEY, AGENTMAIL_FROM_ADDRESS, or APPROVER_EMAIL not set — using MockApprovalSender and MockCustomerMailer"
                );
                (
                    Arc::new(MockApprovalSender) as Arc<dyn ApprovalSender>,
                    Arc::new(MockCustomerMailer) as Arc<dyn CustomerMailer>,
                )
            };

        let ticket_system: Arc<dyn TicketSystem> =
            Arc::new(SqliteTicketSystem::new(db_path).expect("ticket system"));

        let trace_sink: Arc<dyn TraceSink> = match std::env::var("RESPAN_API_KEY")
            .ok()
            .filter(|s| !s.is_empty())
        {
            Some(key) => Arc::new(RespanSink::new(key)),
            None => {
                tracing::info!("RESPAN_API_KEY not set — using NoopSink");
                Arc::new(NoopSink)
            }
        };

        Self {
            engine,
            reader_store,
            registry,
            correlations,
            webhook_secret,
            api_key,
            classifier,
            approval_sender,
            mailer,
            ticket_system,
            trace_sink,
            worker_id,
            metrics: Arc::new(Metrics::new(db_path).expect("metrics store")),
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
        self.recover_inner(true).await
    }

    pub(crate) fn forward_trace(&self, workflow_id: String) {
        spawn_workflow_trace_batch(
            Arc::clone(&self.reader_store),
            Arc::clone(&self.trace_sink),
            self.worker_id.clone(),
            workflow_id,
        );
    }

    /// Periodic sweep: re-admits steps whose lease expired after startup (e.g. a
    /// restart that came back before the dead worker's lease ran out).
    pub async fn sweep_expired_leases(&self) -> Result<usize, EngineError> {
        self.recover_inner(false).await
    }

    async fn recover_inner(&self, trace_all: bool) -> Result<usize, EngineError> {
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
        if trace_all || count > 0 {
            for workflow_id in recovery_trace_targets {
                spawn_workflow_trace_batch(
                    Arc::clone(&self.reader_store),
                    Arc::clone(&self.trace_sink),
                    self.worker_id.clone(),
                    workflow_id,
                );
            }
        }
        Ok(count)
    }
}

#[derive(serde::Serialize)]
struct WorkflowSummary {
    id: String,
    customer_id: String,
    subject: String,
    status: String,
    waiting_step: Option<usize>,
    current_step_name: Option<String>,
    step_count: usize,
    started_at_millis: Option<i64>,
    last_event_at_millis: Option<i64>,
    duration_millis: Option<i64>,
}

fn ticket_workflow_step_names() -> Vec<String> {
    ticket_workflow("_")
        .steps
        .iter()
        .map(|s| s.name.clone())
        .collect()
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn active_step_index_from_events(events: &[Event]) -> Option<usize> {
    let mut current: Option<usize> = None;
    for event in events {
        match event {
            Event::StepStarted { step_index, .. }
            | Event::StepWaiting { step_index, .. }
            | Event::StepResumed { step_index, .. }
            | Event::WorkerRecovered { step_index, .. } => {
                current = Some(*step_index);
            }
            Event::WorkflowCompleted { .. }
            | Event::WorkflowFailed { .. }
            | Event::WorkflowCancelled { .. } => {
                current = None;
            }
            _ => {}
        }
    }
    current
}

fn build_workflow_summary(ticket: Ticket, events_with_ts: Vec<(Event, i64)>) -> WorkflowSummary {
    let step_names = ticket_workflow_step_names();
    let step_count = step_names.len();
    let events: Vec<Event> = events_with_ts.iter().map(|(e, _)| e.clone()).collect();
    let (status, waiting_step) = workflow_status_from_events(&events);

    let started_at_millis = events_with_ts.first().map(|(_, ts)| *ts);
    let last_event_at_millis = events_with_ts.last().map(|(_, ts)| *ts);

    let duration_millis = match (started_at_millis, status.as_str()) {
        (Some(start), "completed" | "failed" | "cancelled") => {
            last_event_at_millis.map(|last| last - start)
        }
        (Some(start), "running" | "waiting") => Some(now_millis() - start),
        _ => None,
    };

    let current_step_index = match status.as_str() {
        "waiting" => waiting_step,
        "running" => active_step_index_from_events(&events),
        _ => None,
    };
    let current_step_name = current_step_index.and_then(|i| step_names.get(i).cloned());

    WorkflowSummary {
        id: ticket.id,
        customer_id: ticket.customer_id,
        subject: ticket.subject,
        status,
        waiting_step,
        current_step_name,
        step_count,
        started_at_millis,
        last_event_at_millis,
        duration_millis,
    }
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

fn workflow_has_terminal_event(events: &[Event]) -> bool {
    events.iter().any(|e| {
        matches!(
            e,
            Event::WorkflowCompleted { .. }
                | Event::WorkflowFailed { .. }
                | Event::WorkflowCancelled { .. }
        )
    })
}

fn ensure_terminal_failure_visible(
    reader_store: &SqliteStore,
    workflow_id: &str,
    err: &EngineError,
) {
    if matches!(err, EngineError::WorkflowFailed { .. }) {
        return;
    }
    let events = match reader_store.load_events(workflow_id) {
        Ok(events) => events,
        Err(store_err) => {
            tracing::error!(
                workflow_id = %workflow_id,
                error = %store_err,
                "load_events failed while recording execution failure"
            );
            return;
        }
    };
    if workflow_has_terminal_event(&events) {
        return;
    }
    let reason = err.to_string();
    if let Err(store_err) = reader_store.append_event(&Event::WorkflowFailed {
        workflow_id: workflow_id.to_string(),
        reason,
    }) {
        tracing::error!(
            workflow_id = %workflow_id,
            error = %store_err,
            "append WorkflowFailed after execution error failed"
        );
    }
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

#[derive(serde::Serialize)]
struct ConfigView {
    classifier: &'static str,
    mailer: &'static str,
    tracing: bool,
    auth_required: bool,
    dev_tools: bool,
}

fn env_set(name: &str) -> bool {
    std::env::var(name).is_ok_and(|v| !v.is_empty())
}

async fn get_config(State(state): State<ApiState>) -> Json<ConfigView> {
    #[cfg(feature = "dev-tools")]
    let dev_tools = dev_kill_allowed();
    #[cfg(not(feature = "dev-tools"))]
    let dev_tools = false;
    Json(ConfigView {
        classifier: if env_set("OPENAI_API_KEY") {
            "openai"
        } else {
            "mock"
        },
        mailer: if env_set("AGENTMAIL_API_KEY")
            && env_set("AGENTMAIL_FROM_ADDRESS")
            && env_set("APPROVER_EMAIL")
        {
            "agentmail"
        } else {
            "mock"
        },
        tracing: env_set("RESPAN_API_KEY"),
        auth_required: state.api_key.is_some(),
        dev_tools,
    })
}

#[cfg(feature = "dev-tools")]
#[derive(serde::Serialize)]
struct EvalResult {
    name: String,
    passed: bool,
}

#[cfg(feature = "dev-tools")]
async fn dev_run_evals() -> Result<Json<Vec<EvalResult>>, StatusCode> {
    if !dev_kill_allowed() {
        return Err(StatusCode::FORBIDDEN);
    }
    let output = tokio::task::spawn_blocking(|| {
        std::process::Command::new("cargo")
            .args(["test", "reliability_evals::", "--", "--test-threads=4"])
            // Evals must run hermetically: the server's auth key and live integrations would leak in.
            .env_remove("DURABLE_AGENT_API_KEY")
            .env_remove("OPENAI_API_KEY")
            .env_remove("AGENTMAIL_API_KEY")
            .env_remove("AGENTMAIL_FROM_ADDRESS")
            .env_remove("APPROVER_EMAIL")
            .env_remove("AGENTMAIL_WEBHOOK_SECRET")
            .env_remove("RESPAN_API_KEY")
            .output()
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let results = stdout
        .lines()
        .filter_map(|line| {
            let rest = line.strip_prefix("test reliability_evals::")?;
            let (name, status) = rest.split_once(" ... ")?;
            Some(EvalResult {
                name: name.trim_start_matches("eval_").to_string(),
                passed: status.trim() == "ok",
            })
        })
        .collect::<Vec<_>>();
    Ok(Json(results))
}

fn bearer_token_matches(header: &str, expected: &str) -> bool {
    header.strip_prefix("Bearer ") == Some(expected)
}

async fn require_api_key(
    State(expected): State<Option<String>>,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    match expected.as_deref() {
        None => Ok(next.run(request).await),
        Some(expected) => {
            let authorized = request
                .headers()
                .get(axum::http::header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .is_some_and(|header| bearer_token_matches(header, expected));
            if authorized {
                Ok(next.run(request).await)
            } else {
                Err(StatusCode::UNAUTHORIZED)
            }
        }
    }
}

fn api_key_from_env() -> Option<String> {
    std::env::var("DURABLE_AGENT_API_KEY")
        .ok()
        .filter(|s| !s.is_empty())
}

fn workflow_management_routes() -> Router<ApiState> {
    Router::new()
        .route("/workflows", post(create_workflow).get(list_workflows))
        .route("/workflow-definition", get(workflow_definition))
        .route("/workflows/{id}", get(get_workflow))
        .route("/workflows/{id}/run", post(run_workflow))
        .route("/workflows/{id}/events", get(workflow_events))
        .route("/workflows/{id}/approve", post(approve_workflow))
        .route("/workflows/{id}/reject", post(reject_workflow))
        .route("/workflows/{id}/cancel", post(cancel_workflow))
        .route("/metrics", get(get_metrics))
}

fn public_routes() -> Router<ApiState> {
    let public = Router::new()
        .route("/webhooks/agentmail", post(agentmail_webhook))
        .route("/config", get(get_config));
    #[cfg(feature = "dev-tools")]
    {
        public
            .route("/dev/kill", post(dev_kill_worker))
            .route("/dev/evals", post(dev_run_evals))
    }
    #[cfg(not(feature = "dev-tools"))]
    {
        public
    }
}

pub fn router_with_api_auth(api_key: Option<String>) -> Router<ApiState> {
    let workflow_routes = workflow_management_routes()
        .route_layer(middleware::from_fn_with_state(api_key, require_api_key));
    Router::new().merge(workflow_routes).merge(public_routes())
}

#[cfg(test)]
pub fn router() -> Router<ApiState> {
    router_with_api_auth(api_key_from_env())
}

async fn get_metrics(State(state): State<ApiState>) -> Result<Json<MetricsView>, StatusCode> {
    let workflows_waiting = state.count_workflows_waiting()?;
    state
        .metrics
        .snapshot(workflows_waiting)
        .map(Json)
        .map_err(|e| {
            tracing::error!(error = %e, "metrics snapshot failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })
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

async fn list_workflows(
    State(state): State<ApiState>,
) -> Result<Json<Vec<WorkflowSummary>>, StatusCode> {
    let ids = state.registry.list().map_err(|e| {
        tracing::error!(error = %e, "registry list failed");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let mut summaries = Vec::with_capacity(ids.len());
    for id in ids {
        let ticket = state.registry.get(&id).map_err(|e| {
            tracing::error!(workflow_id = %id, error = %e, "registry get failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
        let ticket = ticket.ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
        let events_with_ts = state
            .reader_store
            .load_events_with_timestamps(&id)
            .map_err(|e| {
                tracing::error!(workflow_id = %id, error = %e, "load_events_with_timestamps failed");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
        summaries.push(build_workflow_summary(ticket, events_with_ts));
    }
    Ok(Json(summaries))
}

async fn workflow_definition() -> Json<Vec<String>> {
    Json(ticket_workflow_step_names())
}

async fn get_workflow(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<WorkflowSummary>, StatusCode> {
    let ticket = state.registry.get(&id).map_err(|e| {
        tracing::error!(workflow_id = %id, error = %e, "registry get failed");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let ticket = ticket.ok_or(StatusCode::NOT_FOUND)?;

    let events_with_ts = state
        .reader_store
        .load_events_with_timestamps(&id)
        .map_err(|e| {
            tracing::error!(workflow_id = %id, error = %e, "load_events_with_timestamps failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(build_workflow_summary(ticket, events_with_ts)))
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
            ensure_terminal_failure_visible(reader_store.as_ref(), &workflow_id, &e);
        }
        spawn_workflow_trace_batch(reader_store, trace_sink, worker_id, workflow_id);
    });

    state.metrics.inc_workflow_runs();
    Ok(StatusCode::ACCEPTED)
}

fn event_to_json(event: &Event) -> serde_json::Value {
    match event {
        Event::WorkflowStarted { workflow_id } => serde_json::json!({
            "WorkflowStarted": { "workflow_id": workflow_id }
        }),
        Event::StepStarted {
            workflow_id,
            step_index,
            attempt,
        } => serde_json::json!({
            "StepStarted": {
                "workflow_id": workflow_id,
                "step_index": step_index,
                "attempt": attempt,
            }
        }),
        Event::StepCompleted {
            workflow_id,
            step_index,
            output,
        } => serde_json::json!({
            "StepCompleted": {
                "workflow_id": workflow_id,
                "step_index": step_index,
                "output": output,
            }
        }),
        Event::StepFailed {
            workflow_id,
            step_index,
            reason,
        } => serde_json::json!({
            "StepFailed": {
                "workflow_id": workflow_id,
                "step_index": step_index,
                "reason": reason,
            }
        }),
        Event::RetryScheduled {
            workflow_id,
            step_index,
            attempt,
        } => serde_json::json!({
            "RetryScheduled": {
                "workflow_id": workflow_id,
                "step_index": step_index,
                "attempt": attempt,
            }
        }),
        Event::StepWaiting {
            workflow_id,
            step_index,
            reason,
        } => serde_json::json!({
            "StepWaiting": {
                "workflow_id": workflow_id,
                "step_index": step_index,
                "reason": reason,
            }
        }),
        Event::StepResumed {
            workflow_id,
            step_index,
            input,
        } => serde_json::json!({
            "StepResumed": {
                "workflow_id": workflow_id,
                "step_index": step_index,
                "input": input,
            }
        }),
        Event::WorkflowCompleted { workflow_id } => serde_json::json!({
            "WorkflowCompleted": { "workflow_id": workflow_id }
        }),
        Event::WorkflowFailed {
            workflow_id,
            reason,
        } => serde_json::json!({
            "WorkflowFailed": {
                "workflow_id": workflow_id,
                "reason": reason,
            }
        }),
        Event::WorkflowCancelled {
            workflow_id,
            reason,
        } => serde_json::json!({
            "WorkflowCancelled": {
                "workflow_id": workflow_id,
                "reason": reason,
            }
        }),
        Event::WorkerRecovered {
            workflow_id,
            step_index,
        } => serde_json::json!({
            "WorkerRecovered": {
                "workflow_id": workflow_id,
                "step_index": step_index,
            }
        }),
    }
}

async fn workflow_events(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<Vec<serde_json::Value>>, StatusCode> {
    let ticket = state.registry.get(&id).map_err(|e| {
        tracing::error!(workflow_id = %id, error = %e, "registry get failed");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    if ticket.is_none() {
        return Err(StatusCode::NOT_FOUND);
    }

    let events_with_ts = state
        .reader_store
        .load_events_with_timestamps(&id)
        .map_err(|e| {
            tracing::error!(workflow_id = %id, error = %e, "load_events_with_timestamps failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    let out = events_with_ts
        .into_iter()
        .map(|(event, ts_millis)| {
            let mut value = event_to_json(&event);
            value["ts_millis"] = serde_json::Value::from(ts_millis);
            value
        })
        .collect();
    Ok(Json(out))
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
            ensure_terminal_failure_visible(reader_store.as_ref(), &id, &e);
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
    async fn sweep_expired_leases_recovers_without_startup_recovery() {
        use crate::classifier::{Classifier, MockClassifier};

        let path = recovery_test_db_path("sweep-complete");
        let _ = std::fs::remove_file(&path);

        let workflow_id = "wf-sweep";
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
        let swept = state.sweep_expired_leases().await.expect("sweep");
        assert!(swept >= 1, "expected sweep to re-admit the expired step");
        assert_eq!(state.sweep_expired_leases().await.expect("second sweep"), 0);

        let reader = SqliteStore::new(&path).expect("poll store");
        for _ in 0..200 {
            let events = reader.load_events(workflow_id).expect("load_events");
            if workflow_completed(&events) {
                let _ = std::fs::remove_file(&path);
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("workflow did not complete after sweep");
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
    fn step_resumed_event_to_json_includes_input() {
        let event = Event::StepResumed {
            workflow_id: "w".into(),
            step_index: 2,
            input: "approved reply text".into(),
        };
        let value = event_to_json(&event);
        assert_eq!(
            value["StepResumed"]["input"],
            serde_json::Value::String("approved reply text".into())
        );
        assert_eq!(value["StepResumed"]["step_index"], 2);
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
                input: "approved".into(),
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
                input: "approved".into(),
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
    async fn workflow_definition_returns_ticket_step_names() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        let path = recovery_test_db_path("workflow-definition");
        let _ = std::fs::remove_file(&path);
        let app = router().with_state(ApiState::new(&path));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/workflow-definition")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("oneshot");
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let names: Vec<String> = serde_json::from_slice(&body).expect("json");
        assert_eq!(
            names,
            vec![
                "IngestTicket",
                "ClassifyTicket",
                "RequestApproval",
                "SendReply",
                "UpdateTicketSystem",
                "Complete",
            ]
        );

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn workflow_summary_api_lifecycle() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        let path = recovery_test_db_path("workflow-summary");
        let _ = std::fs::remove_file(&path);
        let app = router().with_state(ApiState::new(&path));
        let workflow_id = "wf-summary";
        let ticket_json = format!(
            r#"{{
            "id": "{workflow_id}",
            "customer_id": "cust-summary@example.com",
            "subject": "Summary subject",
            "body": "Body text"
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
        let pending: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(pending["status"], "pending");
        assert!(pending["started_at_millis"].is_null());
        assert!(pending["duration_millis"].is_null());
        assert_eq!(pending["customer_id"], "cust-summary@example.com");
        assert_eq!(pending["subject"], "Summary subject");
        assert_eq!(
            pending["step_count"],
            ticket_workflow("_").steps.len() as i64
        );

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

        let mut list_summary: Option<serde_json::Value> = None;
        for _ in 0..200 {
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
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("body");
            let list: Vec<serde_json::Value> = serde_json::from_slice(&body).expect("json");
            assert_eq!(list.len(), 1);
            assert_eq!(list[0]["id"], workflow_id);
            if !list[0]["started_at_millis"].is_null() {
                list_summary = Some(list[0].clone());
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let list_summary = list_summary.expect("workflow should have started_at_millis after run");
        assert_eq!(list_summary["customer_id"], "cust-summary@example.com");
        assert_eq!(list_summary["subject"], "Summary subject");
        assert_eq!(
            list_summary["step_count"],
            ticket_workflow("_").steps.len() as i64
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
        let detail: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(detail["id"], workflow_id);
        assert_eq!(detail["customer_id"], list_summary["customer_id"]);
        assert_eq!(detail["subject"], list_summary["subject"]);
        assert_eq!(
            detail["started_at_millis"],
            list_summary["started_at_millis"]
        );
        assert!(!detail["started_at_millis"].is_null());

        let _ = std::fs::remove_file(&path);
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

    #[tokio::test]
    async fn workflow_events_unknown_id_returns_not_found() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        let path = recovery_test_db_path("events-unknown");
        let _ = std::fs::remove_file(&path);
        let app = router().with_state(ApiState::new(&path));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/workflows/does-not-exist/events")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("oneshot");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn approve_without_mounted_workflow_surfaces_failed_status_via_http() {
        use crate::classifier::{Classifier, MockClassifier};
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        let path = recovery_test_db_path("approve-unmounted");
        let _ = std::fs::remove_file(&path);

        let workflow_id = "wf-approve-unmounted";
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

        let app = router().with_state(ApiState::new(&path));

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
            if view["status"] == "failed" {
                let reader = SqliteStore::new(&path).expect("reader");
                let events = reader.load_events(workflow_id).expect("events");
                assert!(workflow_failed(&events));
                let _ = std::fs::remove_file(&path);
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        let reader = SqliteStore::new(&path).expect("reader");
        let events = reader.load_events(workflow_id).expect("events");
        panic!("workflow stayed non-failed after unmounted approve; events: {events:?}");
    }

    #[tokio::test]
    async fn durable_agent_api_key_protects_workflow_routes_when_set() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        let path = recovery_test_db_path("api-key-auth");
        let _ = std::fs::remove_file(&path);
        let state = ApiState::new_with_api_key(&path, "test-api-key-secret");
        let app = router_with_api_auth(state.api_key.clone()).with_state(state);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("oneshot");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .header("authorization", "Bearer wrong-key")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("oneshot");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .header("authorization", "Bearer test-api-key-secret")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("oneshot");
        assert_eq!(response.status(), StatusCode::OK);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/workflow-definition")
                    .header("authorization", "Bearer test-api-key-secret")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("oneshot");
        assert_eq!(response.status(), StatusCode::OK);

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn durable_agent_api_key_does_not_protect_webhook_or_dev_kill() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        let path = recovery_test_db_path("api-key-public-routes");
        let _ = std::fs::remove_file(&path);
        let state = ApiState::new_with_api_key(&path, "test-api-key-secret");
        let app = router_with_api_auth(state.api_key.clone()).with_state(state);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/webhooks/agentmail")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("oneshot");
        assert_ne!(response.status(), StatusCode::UNAUTHORIZED);

        #[cfg(feature = "dev-tools")]
        {
            unsafe { std::env::remove_var("DURABLE_AGENT_ALLOW_DEV_KILL") };
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
        }

        let _ = std::fs::remove_file(&path);
    }
}
