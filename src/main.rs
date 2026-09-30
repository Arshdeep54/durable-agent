mod api;
mod approval;
mod approval_correlations;
mod classifier;
mod domain;
mod registry;
mod steps;
mod ticket_system;
mod tracing_sink;
mod webhook;
mod workflow_def;

#[cfg(test)]
mod reliability_evals;

use api::ApiState;
use axum::{
    Router,
    http::StatusCode,
    routing::{get, get_service},
};
use tower_http::services::ServeFile;

const DB_PATH: &str = "durable-agent.db";

fn app(state: ApiState) -> Router {
    Router::new()
        .route("/", get_service(ServeFile::new("web/index.html")))
        .merge(
            Router::new()
                .route("/health", get(|| async { (StatusCode::OK, "ok") }))
                .merge(api::router())
                .with_state(state),
        )
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let state = ApiState::new(DB_PATH);
    match state.recover_pending_workflows().await {
        Ok(count) => tracing::info!(count, "startup recovery: re-admitted interrupted step(s)"),
        Err(e) => tracing::error!(error = %e, "startup recovery failed"),
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080")
        .await
        .expect("bind 127.0.0.1:8080");
    axum::serve(listener, app(state)).await.expect("serve");
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use std::time::Duration;
    use tower::ServiceExt;

    fn test_db_path(suffix: &str) -> String {
        std::env::temp_dir()
            .join(format!(
                "durable-agent-api-test-{}-{}.db",
                std::process::id(),
                suffix
            ))
            .to_str()
            .expect("temp db path utf8")
            .to_string()
    }

    fn test_app(suffix: &str) -> Router {
        let path = test_db_path(suffix);
        let _ = std::fs::remove_file(&path);
        app(ApiState::new(&path))
    }

    async fn poll_status(
        app: &Router,
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

    #[tokio::test]
    async fn index_returns_html() {
        let response = test_app("index")
            .oneshot(
                Request::builder()
                    .uri("/")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("oneshot");

        assert_eq!(response.status(), StatusCode::OK);
        let content_type = response
            .headers()
            .get("content-type")
            .expect("content-type")
            .to_str()
            .expect("content-type utf8");
        assert!(
            content_type.starts_with("text/html"),
            "expected text/html, got {content_type}"
        );
    }

    #[cfg(not(feature = "dev-tools"))]
    #[tokio::test]
    async fn dev_kill_route_absent_without_dev_tools_feature() {
        let response = test_app("dev-kill-absent")
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/dev/kill")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("oneshot");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn health_returns_ok() {
        let response = test_app("health")
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("oneshot");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn workflow_api_lifecycle() {
        let app = test_app("lifecycle");

        let ticket_json = r#"{
            "id": "wf-1",
            "customer_id": "cust@example.com",
            "subject": "Need help",
            "body": "Just saying hello"
        }"#;

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
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let created: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(created["id"], "wf-1");
        assert_eq!(created["customer_id"], "cust@example.com");

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
        assert_eq!(response.status(), StatusCode::CONFLICT);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/workflows/wf-1")
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
        assert_eq!(view["id"], "wf-1");
        assert_eq!(view["status"], "pending");

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/workflows/wf-1/run")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("oneshot");
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let waiting = poll_status(&app, "wf-1", "waiting", 200).await;
        assert_eq!(waiting["waiting_step"], 2);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/workflows/missing")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("oneshot");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

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
        assert_eq!(list[0], "wf-1");

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/workflows/wf-1/events")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("oneshot");
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let events: Vec<serde_json::Value> = serde_json::from_slice(&body).expect("json");
        assert!(!events.is_empty());

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/workflows/wf-1/approve")
                    .body(Body::from("approved reply text"))
                    .expect("request"),
            )
            .await
            .expect("oneshot");
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let completed = poll_status(&app, "wf-1", "completed", 200).await;
        assert_eq!(completed["id"], "wf-1");
        assert!(completed["waiting_step"].is_null());
    }

    #[tokio::test]
    async fn workflow_reject_via_http_fails_workflow() {
        let app = test_app("reject-http");

        let ticket_json = r#"{
            "id": "wf-reject",
            "customer_id": "cust@example.com",
            "subject": "Need help",
            "body": "Just saying hello"
        }"#;

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
                    .uri("/workflows/wf-reject/run")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("oneshot");
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let waiting = poll_status(&app, "wf-reject", "waiting", 200).await;
        assert_eq!(waiting["waiting_step"], 2);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/workflows/wf-reject/reject")
                    .body(Body::from("not needed"))
                    .expect("request"),
            )
            .await
            .expect("oneshot");
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let failed = poll_status(&app, "wf-reject", "failed", 200).await;
        assert_eq!(failed["id"], "wf-reject");
        assert!(failed["waiting_step"].is_null());
    }

    #[tokio::test]
    async fn workflow_second_approve_after_completion_returns_not_found() {
        let app = test_app("second-approve");

        let ticket_json = r#"{
            "id": "wf-twice",
            "customer_id": "cust@example.com",
            "subject": "Need help",
            "body": "Just saying hello"
        }"#;

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
                    .uri("/workflows/wf-twice/run")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("oneshot");
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        poll_status(&app, "wf-twice", "waiting", 200).await;

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/workflows/wf-twice/approve")
                    .body(Body::from("approved reply text"))
                    .expect("request"),
            )
            .await
            .expect("oneshot");
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        poll_status(&app, "wf-twice", "completed", 200).await;

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/workflows/wf-twice/approve")
                    .body(Body::from("again"))
                    .expect("request"),
            )
            .await
            .expect("oneshot");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/workflows/wf-twice/reject")
                    .body(Body::from("too late"))
                    .expect("request"),
            )
            .await
            .expect("oneshot");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn workflow_approve_or_reject_before_ever_run_returns_not_found() {
        let app = test_app("approve-before-run");

        let ticket_json = r#"{
            "id": "wf-never-run",
            "customer_id": "cust@example.com",
            "subject": "Need help",
            "body": "Just saying hello"
        }"#;

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

        // No /run call: the workflow has no events at all. Approving or
        // rejecting a workflow that was never run — a stale/invalid
        // approval attempt — must not be accepted.
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/workflows/wf-never-run/approve")
                    .body(Body::from("approved"))
                    .expect("request"),
            )
            .await
            .expect("oneshot");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/workflows/wf-never-run/reject")
                    .body(Body::from("rejected"))
                    .expect("request"),
            )
            .await
            .expect("oneshot");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    async fn post_cancel(app: &Router, id: &str, body: &str) -> StatusCode {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/workflows/{id}/cancel"))
                    .body(Body::from(body.to_string()))
                    .expect("request"),
            )
            .await
            .expect("oneshot");
        response.status()
    }

    fn count_workflow_cancelled_events(events: &[serde_json::Value]) -> usize {
        events
            .iter()
            .filter(|e| e.get("WorkflowCancelled").is_some())
            .count()
    }

    #[tokio::test]
    async fn workflow_cancel_fresh_never_run() {
        let app = test_app("cancel-fresh");

        let ticket_json = r#"{
            "id": "wf-cancel-fresh",
            "customer_id": "cust@example.com",
            "subject": "Need help",
            "body": "Just saying hello"
        }"#;

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

        assert_eq!(
            post_cancel(&app, "wf-cancel-fresh", "").await,
            StatusCode::OK
        );

        let view = poll_status(&app, "wf-cancel-fresh", "cancelled", 50).await;
        assert_eq!(view["id"], "wf-cancel-fresh");
        assert!(view["waiting_step"].is_null());
    }

    #[tokio::test]
    async fn workflow_cancel_twice_is_idempotent() {
        let app = test_app("cancel-twice");

        let ticket_json = r#"{
            "id": "wf-cancel-twice",
            "customer_id": "cust@example.com",
            "subject": "Need help",
            "body": "Just saying hello"
        }"#;

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

        assert_eq!(
            post_cancel(&app, "wf-cancel-twice", "once").await,
            StatusCode::OK
        );
        assert_eq!(
            post_cancel(&app, "wf-cancel-twice", "again").await,
            StatusCode::OK
        );

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/workflows/wf-cancel-twice/events")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("oneshot");
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let events: Vec<serde_json::Value> = serde_json::from_slice(&body).expect("json");
        assert_eq!(count_workflow_cancelled_events(&events), 1);
    }

    #[tokio::test]
    async fn workflow_cancel_unknown_returns_not_found() {
        let app = test_app("cancel-missing");
        assert_eq!(
            post_cancel(&app, "no-such-workflow", "").await,
            StatusCode::NOT_FOUND
        );
    }

    #[tokio::test]
    async fn workflow_cancel_while_waiting_blocks_approve() {
        let app = test_app("cancel-wait-approve");

        let ticket_json = r#"{
            "id": "wf-cancel-wait",
            "customer_id": "cust@example.com",
            "subject": "Need help",
            "body": "Just saying hello"
        }"#;

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
                    .uri("/workflows/wf-cancel-wait/run")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("oneshot");
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        poll_status(&app, "wf-cancel-wait", "waiting", 200).await;

        assert_eq!(
            post_cancel(&app, "wf-cancel-wait", "no longer needed").await,
            StatusCode::OK
        );

        let cancelled = poll_status(&app, "wf-cancel-wait", "cancelled", 200).await;
        assert_eq!(cancelled["id"], "wf-cancel-wait");
        assert!(cancelled["waiting_step"].is_null());

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/workflows/wf-cancel-wait/approve")
                    .body(Body::from("too late"))
                    .expect("request"),
            )
            .await
            .expect("oneshot");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    async fn fetch_metrics(app: &Router) -> serde_json::Value {
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
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        serde_json::from_slice(&body).expect("metrics json")
    }

    #[tokio::test]
    async fn metrics_reflect_create_and_run() {
        let app = test_app("metrics-create-run");

        let before = fetch_metrics(&app).await;
        assert_eq!(before["workflows_created"], 0);
        assert_eq!(before["workflow_runs"], 0);

        let ticket_json = r#"{
            "id": "wf-metrics",
            "customer_id": "cust@example.com",
            "subject": "Need help",
            "body": "Just saying hello"
        }"#;

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

        let after_create = fetch_metrics(&app).await;
        assert_eq!(after_create["workflows_created"], 1);
        assert_eq!(after_create["workflow_runs"], 0);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/workflows/wf-metrics/run")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("oneshot");
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let after_run = fetch_metrics(&app).await;
        assert_eq!(after_run["workflows_created"], 1);
        assert_eq!(after_run["workflow_runs"], 1);
    }

    #[tokio::test]
    async fn metrics_workflows_waiting_gauge() {
        let app = test_app("metrics-waiting");

        let ticket_json = r#"{
            "id": "wf-metrics-wait",
            "customer_id": "cust@example.com",
            "subject": "Need help",
            "body": "Just saying hello"
        }"#;

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
                    .uri("/workflows/wf-metrics-wait/run")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("oneshot");
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        poll_status(&app, "wf-metrics-wait", "waiting", 200).await;

        let metrics = fetch_metrics(&app).await;
        assert_eq!(metrics["workflows_waiting"], 1);
    }
}
