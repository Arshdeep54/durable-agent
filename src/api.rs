use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
};
use serde::Deserialize;

#[derive(Clone, serde::Serialize)]
pub struct WorkflowRecord {
    pub id: String,
    pub status: String, // "pending" | "running" | "waiting" | "completed"
}

#[derive(Clone)]
pub struct ApiState {
    pub workflows: std::sync::Arc<std::sync::Mutex<Vec<WorkflowRecord>>>,
}

impl ApiState {
    pub fn new() -> Self {
        Self {
            workflows: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }
}

#[derive(Deserialize)]
struct CreateWorkflowBody {
    id: String,
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
    Json(body): Json<CreateWorkflowBody>,
) -> Result<(StatusCode, Json<WorkflowRecord>), StatusCode> {
    let mut workflows = state.workflows.lock().expect("workflows lock");
    if workflows.iter().any(|w| w.id == body.id) {
        return Err(StatusCode::CONFLICT);
    }
    let record = WorkflowRecord {
        id: body.id,
        status: "pending".to_string(),
    };
    workflows.push(record.clone());
    Ok((StatusCode::CREATED, Json(record)))
}

async fn list_workflows(State(state): State<ApiState>) -> Json<Vec<WorkflowRecord>> {
    let workflows = state.workflows.lock().expect("workflows lock");
    Json(workflows.clone())
}

async fn get_workflow(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<WorkflowRecord>, StatusCode> {
    let workflows = state.workflows.lock().expect("workflows lock");
    workflows
        .iter()
        .find(|w| w.id == id)
        .cloned()
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn run_workflow(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<WorkflowRecord>, StatusCode> {
    let mut workflows = state.workflows.lock().expect("workflows lock");
    let record = workflows
        .iter_mut()
        .find(|w| w.id == id)
        .ok_or(StatusCode::NOT_FOUND)?;
    record.status = "running".to_string();
    Ok(Json(record.clone()))
}

async fn workflow_events() -> Json<Vec<serde_json::Value>> {
    Json(Vec::new())
}

async fn approve_workflow(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<WorkflowRecord>, StatusCode> {
    let mut workflows = state.workflows.lock().expect("workflows lock");
    let record = workflows
        .iter_mut()
        .find(|w| w.id == id)
        .ok_or(StatusCode::NOT_FOUND)?;
    record.status = "completed".to_string();
    Ok(Json(record.clone()))
}
