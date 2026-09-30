mod api;
mod approval;
mod classifier;
mod domain;
mod registry;
mod steps;
mod ticket_system;
mod workflow_def;

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
    let state = ApiState::new(DB_PATH);
    match state.recover_pending_workflows().await {
        Ok(count) => println!("startup recovery: re-admitted {count} interrupted step(s)"),
        Err(e) => eprintln!("startup recovery failed: {e}"),
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
}
