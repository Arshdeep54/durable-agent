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

fn app() -> Router {
    let state = ApiState::new();
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
    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080")
        .await
        .expect("bind 127.0.0.1:8080");
    axum::serve(listener, app()).await.expect("serve");
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    #[tokio::test]
    async fn index_returns_html() {
        let response = app()
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
        let response = app()
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
        let app = app();

        let create = |id: &str| {
            Request::builder()
                .method("POST")
                .uri("/workflows")
                .header("content-type", "application/json")
                .body(Body::from(format!(r#"{{"id":"{}"}}"# , id)))
                .expect("request")
        };

        let response = app
            .clone()
            .oneshot(create("wf-1"))
            .await
            .expect("oneshot");
        assert_eq!(response.status(), StatusCode::CREATED);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let record: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(record["id"], "wf-1");
        assert_eq!(record["status"], "pending");

        let response = app
            .clone()
            .oneshot(create("wf-1"))
            .await
            .expect("oneshot");
        assert_eq!(response.status(), StatusCode::CONFLICT);

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
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let record: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(record["status"], "running");

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
        let record: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(record["id"], "wf-1");
        assert_eq!(record["status"], "running");

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
        assert_eq!(list[0]["id"], "wf-1");

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
        assert!(events.is_empty());

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/workflows/wf-1/approve")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("oneshot");
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let record: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(record["status"], "completed");
    }
}
