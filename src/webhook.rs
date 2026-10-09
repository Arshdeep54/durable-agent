use std::time::{SystemTime, UNIX_EPOCH};

use agentq::{EngineError, StoreError};
use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use hmac::{Hmac, Mac};
use serde::Deserialize;
use sha2::Sha256;

use crate::api::ApiState;

type HmacSha256 = Hmac<Sha256>;

const TIMESTAMP_TOLERANCE_SECS: i64 = 300;

pub fn decode_signing_secret(secret: &str) -> Result<Vec<u8>, ()> {
    let encoded = secret.strip_prefix("whsec_").ok_or(())?;
    STANDARD.decode(encoded).map_err(|_| ())
}

pub fn compute_svix_signature(
    secret_bytes: &[u8],
    svix_id: &str,
    svix_timestamp: &str,
    body: &[u8],
) -> String {
    let signed = format!(
        "{}.{}.{}",
        svix_id,
        svix_timestamp,
        String::from_utf8_lossy(body)
    );
    let mut mac = HmacSha256::new_from_slice(secret_bytes).expect("HMAC key length");
    mac.update(signed.as_bytes());
    STANDARD.encode(mac.finalize().into_bytes())
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

pub fn verify_svix_headers(
    secret: &str,
    svix_id: &str,
    svix_timestamp: &str,
    svix_signature: &str,
    body: &[u8],
) -> Result<(), ()> {
    let secret_bytes = decode_signing_secret(secret)?;

    let ts: i64 = svix_timestamp.parse().map_err(|_| ())?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ())?
        .as_secs() as i64;
    if (now - ts).abs() > TIMESTAMP_TOLERANCE_SECS {
        return Err(());
    }

    let expected = compute_svix_signature(&secret_bytes, svix_id, svix_timestamp, body);
    let expected_bytes = expected.as_bytes();

    for part in svix_signature.split(' ') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let Some(candidate_b64) = part.strip_prefix("v1,") else {
            continue;
        };
        if constant_time_eq(expected_bytes, candidate_b64.as_bytes()) {
            return Ok(());
        }
    }

    Err(())
}

#[derive(Debug, Deserialize)]
struct AgentMailWebhook {
    event_type: String,
    #[serde(default)]
    message: Option<AgentMailMessage>,
}

#[derive(Debug, Deserialize)]
struct AgentMailMessage {
    #[serde(default)]
    thread_id: Option<String>,
    #[serde(default)]
    message_id: Option<String>,
    #[serde(default)]
    text: Option<String>,
}

fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

/// Drops the quoted thread mail clients append below a reply.
/// ponytail: only recognizes `>` quotes and "On ... wrote:" headers (Gmail, Apple Mail); Outlook's "From:" block is not stripped.
fn strip_quoted_reply(text: &str) -> String {
    let lines: Vec<&str> = text.lines().collect();
    let end = lines
        .iter()
        .enumerate()
        .position(|(i, line)| {
            let line = line.trim();
            line.starts_with('>')
                || (line.starts_with("On ")
                    && (line.ends_with("wrote:")
                        || lines
                            .get(i + 1)
                            .is_some_and(|n| n.trim().ends_with("wrote:"))))
        })
        .unwrap_or(lines.len());
    lines[..end].join("\n").trim().to_string()
}

fn resume_input_from_reply(text: &str) -> String {
    let text = strip_quoted_reply(text);
    let text = text.as_str();
    if text.to_ascii_lowercase().contains("reject") {
        let first_line = text.lines().next().unwrap_or(text).trim();
        if first_line.is_empty() {
            "rejected".to_string()
        } else {
            format!("rejected:{first_line}")
        }
    } else if text.trim().is_empty() {
        "approved".to_string()
    } else {
        text.trim().to_string()
    }
}

fn resume_error_is_benign(err: &EngineError) -> bool {
    match err {
        EngineError::Store(StoreError::Backend(msg)) => {
            msg == "step is not waiting" || msg == "workflow is cancelled"
        }
        EngineError::WorkflowFailed { .. } => true,
        _ => false,
    }
}

pub async fn agentmail_webhook(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, StatusCode> {
    let secret = state.webhook_secret.as_deref().filter(|s| !s.is_empty());
    let secret = secret.ok_or(StatusCode::UNAUTHORIZED)?;

    let svix_id = header_value(&headers, "svix-id").ok_or(StatusCode::UNAUTHORIZED)?;
    let svix_timestamp =
        header_value(&headers, "svix-timestamp").ok_or(StatusCode::UNAUTHORIZED)?;
    let svix_signature =
        header_value(&headers, "svix-signature").ok_or(StatusCode::UNAUTHORIZED)?;

    verify_svix_headers(secret, &svix_id, &svix_timestamp, &svix_signature, &body)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;

    if state
        .correlations
        .is_webhook_processed(&svix_id)
        .map_err(|e| {
            tracing::error!(error = %e, "processed_webhooks check failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
    {
        return Ok(StatusCode::OK);
    }

    let payload: AgentMailWebhook = serde_json::from_slice(&body).map_err(|e| {
        tracing::error!(error = %e, "agentmail webhook json parse failed");
        StatusCode::BAD_REQUEST
    })?;

    if payload.event_type != "message.received" {
        return Ok(StatusCode::OK);
    }

    let message = payload.message.ok_or(StatusCode::BAD_REQUEST)?;
    let correlation_key = message
        .thread_id
        .filter(|s| !s.is_empty())
        .or(message.message_id.filter(|s| !s.is_empty()));

    let Some(correlation_key) = correlation_key else {
        return Ok(StatusCode::OK);
    };

    let Some((workflow_id, step_index)) =
        state.correlations.lookup(&correlation_key).map_err(|e| {
            tracing::error!(error = %e, "correlation lookup failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
    else {
        return Ok(StatusCode::OK);
    };

    let reply_text = message.text.as_deref().unwrap_or("");
    let input = resume_input_from_reply(reply_text);

    state.metrics.inc_webhooks_processed();
    let resume_result = state.engine.resume(&workflow_id, step_index, input).await;

    match resume_result {
        Ok(()) => state.forward_trace(workflow_id),
        Err(e) if resume_error_is_benign(&e) => {
            tracing::warn!(
                workflow_id = %workflow_id,
                step_index,
                error = %e,
                "agentmail webhook resume benign no-op"
            );
        }
        Err(e) => {
            tracing::error!(
                workflow_id = %workflow_id,
                step_index,
                error = %e,
                "agentmail webhook resume failed"
            );
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    }

    state
        .correlations
        .mark_webhook_processed(&svix_id)
        .map_err(|e| {
            tracing::error!(error = %e, "mark_webhook_processed failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(StatusCode::OK)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::api::{ApiState, router};
    use agentq::{DurableStore, Event, Priority, Queue, SqliteStore, WorkflowEngine};
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use std::time::Duration;
    use tower::ServiceExt;

    const TEST_SECRET: &str = "whsec_aGVsbG8=";

    #[test]
    fn gmail_reply_ignores_quoted_thread() {
        let gmail = "approved\r\n\r\nOn Fri, Oct 9, 2026 at 4:36 AM AgentMail <\r\nsupport@agentmail.to> wrote:\r\n\r\n> Draft reply:\r\n> Please do not reject this.\r\n";
        assert_eq!(resume_input_from_reply(gmail), "approved");
        let edited = "Replacement ships today.\nThanks!\n\n> quoted line\n";
        assert_eq!(
            resume_input_from_reply(edited),
            "Replacement ships today.\nThanks!"
        );
        let rejected = "reject: wrong tone\n\nOn Fri, Oct 9 AgentMail wrote:\n> Draft\n";
        assert_eq!(
            resume_input_from_reply(rejected),
            "rejected:reject: wrong tone"
        );
    }

    fn test_db_path(label: &str) -> String {
        std::env::temp_dir()
            .join(format!(
                "durable-agent-webhook-test-{}-{}.db",
                std::process::id(),
                label
            ))
            .to_str()
            .expect("path")
            .to_string()
    }

    fn sign_payload(secret: &str, svix_id: &str, svix_timestamp: &str, body: &[u8]) -> String {
        let secret_bytes = decode_signing_secret(secret).expect("secret");
        let sig = compute_svix_signature(&secret_bytes, svix_id, svix_timestamp, body);
        format!("v1,{sig}")
    }

    fn now_timestamp() -> String {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("epoch")
            .as_secs()
            .to_string()
    }

    async fn seed_waiting_workflow(
        path: &str,
        workflow_id: &str,
        thread_id: &str,
    ) -> Arc<WorkflowEngine<SqliteStore>> {
        use crate::approval_correlations::ApprovalCorrelations;
        use crate::domain::Ticket;
        use crate::registry::WorkflowRegistry;

        let ticket = Ticket {
            id: workflow_id.to_string(),
            customer_id: "customer@example.com".into(),
            subject: "Need help".into(),
            body: "hello".into(),
        };

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
            "webhook-test-worker".into(),
            Duration::from_secs(60),
            Priority::High,
        ));

        use crate::approval::{MockApprovalSender, MockCustomerMailer};
        use crate::classifier::MockClassifier;
        use crate::steps::build_step_bodies;
        use crate::ticket_system::InMemoryTicketSystem;
        use crate::workflow_def::ticket_workflow;

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

    #[tokio::test]
    async fn valid_approval_webhook_resumes_workflow() {
        let path = test_db_path("approve");
        let _ = std::fs::remove_file(&path);
        let thread_id = "thd_approve";
        let workflow_id = "wf-webhook-approve";
        let engine = seed_waiting_workflow(&path, workflow_id, thread_id).await;

        let mut state = ApiState::new_with_webhook_secret(&path, TEST_SECRET);
        state.engine = engine;

        let body = format!(
            r#"{{"event_type":"message.received","event_id":"evt_1","message":{{"thread_id":"{thread_id}","message_id":"<reply@agentmail.to>","text":"Looks good, ship it."}}}}"#,
        );
        let svix_id = "svix_approve_1";
        let ts = now_timestamp();
        let sig = sign_payload(TEST_SECRET, svix_id, &ts, body.as_bytes());

        let app = router().with_state(state);
        let response = app
            .oneshot(webhook_request(&body, svix_id, &ts, &sig))
            .await
            .expect("oneshot");
        assert_eq!(response.status(), StatusCode::OK);

        let reader = SqliteStore::new(&path).expect("reader");
        for _ in 0..200 {
            let events = reader.load_events(workflow_id).expect("events");
            if workflow_completed(&events) {
                let _ = std::fs::remove_file(&path);
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("workflow did not complete");
    }

    struct CapturingSink(std::sync::Mutex<Vec<String>>);

    impl crate::tracing_sink::TraceSink for CapturingSink {
        fn record_batch(&self, spans: Vec<crate::tracing_sink::TraceSpan>) {
            self.0
                .lock()
                .expect("spans")
                .extend(spans.into_iter().map(|s| s.name));
        }
    }

    #[tokio::test]
    async fn approval_webhook_forwards_completed_trace() {
        let path = test_db_path("approve-trace");
        let _ = std::fs::remove_file(&path);
        let thread_id = "thd_approve_trace";
        let workflow_id = "wf-webhook-approve-trace";
        let engine = seed_waiting_workflow(&path, workflow_id, thread_id).await;

        let sink = Arc::new(CapturingSink(std::sync::Mutex::new(Vec::new())));
        let mut state = ApiState::new_with_webhook_secret_and_trace_sink(
            &path,
            TEST_SECRET,
            Arc::clone(&sink) as Arc<dyn crate::tracing_sink::TraceSink>,
        );
        state.engine = engine;

        let body = format!(
            r#"{{"event_type":"message.received","message":{{"thread_id":"{thread_id}","text":"approved"}}}}"#,
        );
        let ts = now_timestamp();
        let sig = sign_payload(TEST_SECRET, "svix_trace_1", &ts, body.as_bytes());
        let response = router()
            .with_state(state)
            .oneshot(webhook_request(&body, "svix_trace_1", &ts, &sig))
            .await
            .expect("oneshot");
        assert_eq!(response.status(), StatusCode::OK);

        for _ in 0..200 {
            if sink
                .0
                .lock()
                .expect("spans")
                .iter()
                .any(|n| n == "WorkflowCompleted")
            {
                let _ = std::fs::remove_file(&path);
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("no WorkflowCompleted span forwarded after webhook approval");
    }

    #[tokio::test]
    async fn rejection_word_webhook_fails_workflow() {
        let path = test_db_path("reject");
        let _ = std::fs::remove_file(&path);
        let thread_id = "thd_reject";
        let workflow_id = "wf-webhook-reject";
        let engine = seed_waiting_workflow(&path, workflow_id, thread_id).await;

        let mut state = ApiState::new_with_webhook_secret(&path, TEST_SECRET);
        state.engine = engine;

        let body = format!(
            r#"{{"event_type":"message.received","event_id":"evt_2","message":{{"thread_id":"{thread_id}","text":"Please reject this draft"}}}}"#,
        );
        let svix_id = "svix_reject_1";
        let ts = now_timestamp();
        let sig = sign_payload(TEST_SECRET, svix_id, &ts, body.as_bytes());

        let app = router().with_state(state);
        let response = app
            .oneshot(webhook_request(&body, svix_id, &ts, &sig))
            .await
            .expect("oneshot");
        assert_eq!(response.status(), StatusCode::OK);

        let reader = SqliteStore::new(&path).expect("reader");
        for _ in 0..200 {
            let events = reader.load_events(workflow_id).expect("events");
            if workflow_failed(&events) {
                let _ = std::fs::remove_file(&path);
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("workflow did not fail");
    }

    #[tokio::test]
    async fn malformed_json_returns_bad_request() {
        let path = test_db_path("malformed");
        let _ = std::fs::remove_file(&path);
        let state = ApiState::new_with_webhook_secret(&path, TEST_SECRET);
        let body = "not-json";
        let svix_id = "svix_bad_json";
        let ts = now_timestamp();
        let sig = sign_payload(TEST_SECRET, svix_id, &ts, body.as_bytes());

        let app = router().with_state(state);
        let response = app
            .oneshot(webhook_request(body, svix_id, &ts, &sig))
            .await
            .expect("oneshot");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn invalid_signature_returns_unauthorized_without_resume() {
        let path = test_db_path("bad_sig");
        let _ = std::fs::remove_file(&path);
        let thread_id = "thd_bad_sig";
        let workflow_id = "wf-bad-sig";
        let engine = seed_waiting_workflow(&path, workflow_id, thread_id).await;

        let mut state = ApiState::new_with_webhook_secret(&path, TEST_SECRET);
        state.engine = engine;

        let body = format!(
            r#"{{"event_type":"message.received","message":{{"thread_id":"{thread_id}","text":"approved"}}}}"#,
        );
        let svix_id = "svix_bad_sig";
        let ts = now_timestamp();

        let app = router().with_state(state);
        let response = app
            .oneshot(webhook_request(&body, svix_id, &ts, "v1,wrong"))
            .await
            .expect("oneshot");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let reader = SqliteStore::new(&path).expect("reader");
        let events = reader.load_events(workflow_id).expect("events");
        assert_eq!(count_step_resumed(&events, 2), 0);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn stale_timestamp_rejected() {
        let path = test_db_path("stale");
        let _ = std::fs::remove_file(&path);
        let state = ApiState::new_with_webhook_secret(&path, TEST_SECRET);
        let body = r#"{"event_type":"message.received","message":{"thread_id":"x","text":"ok"}}"#;
        let svix_id = "svix_stale";
        let ts = "1000000000";
        let sig = sign_payload(TEST_SECRET, svix_id, ts, body.as_bytes());

        let app = router().with_state(state);
        let response = app
            .oneshot(webhook_request(body, svix_id, ts, &sig))
            .await
            .expect("oneshot");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn duplicate_svix_id_resumes_at_most_once() {
        let path = test_db_path("dup");
        let _ = std::fs::remove_file(&path);
        let thread_id = "thd_dup";
        let workflow_id = "wf-dup";
        let engine = seed_waiting_workflow(&path, workflow_id, thread_id).await;

        let mut state = ApiState::new_with_webhook_secret(&path, TEST_SECRET);
        state.engine = engine;

        let body = format!(
            r#"{{"event_type":"message.received","message":{{"thread_id":"{thread_id}","text":"approved"}}}}"#,
        );
        let svix_id = "svix_dup";
        let ts = now_timestamp();
        let sig = sign_payload(TEST_SECRET, svix_id, &ts, body.as_bytes());

        let app = router().with_state(state);
        let req = webhook_request(&body, svix_id, &ts, &sig);
        let r1 = app.clone().oneshot(req).await.expect("first");
        assert_eq!(r1.status(), StatusCode::OK);
        let r2 = app
            .clone()
            .oneshot(webhook_request(&body, svix_id, &ts, &sig))
            .await
            .expect("second");
        assert_eq!(r2.status(), StatusCode::OK);

        let reader = SqliteStore::new(&path).expect("reader");
        for _ in 0..200 {
            let events = reader.load_events(workflow_id).expect("events");
            if workflow_completed(&events) {
                assert_eq!(count_step_resumed(&events, 2), 1);
                let _ = std::fs::remove_file(&path);
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("workflow did not complete");
    }

    #[tokio::test]
    async fn unknown_correlation_returns_ok_without_resume() {
        let path = test_db_path("unknown");
        let _ = std::fs::remove_file(&path);
        let workflow_id = "wf-unknown";
        let engine = seed_waiting_workflow(&path, workflow_id, "thd_real").await;

        let mut state = ApiState::new_with_webhook_secret(&path, TEST_SECRET);
        state.engine = engine;

        let body = r#"{"event_type":"message.received","message":{"thread_id":"thd_unknown","text":"ok"}}"#;
        let svix_id = "svix_unknown";
        let ts = now_timestamp();
        let sig = sign_payload(TEST_SECRET, svix_id, &ts, body.as_bytes());

        let app = router().with_state(state);
        let response = app
            .oneshot(webhook_request(body, svix_id, &ts, &sig))
            .await
            .expect("oneshot");
        assert_eq!(response.status(), StatusCode::OK);

        let reader = SqliteStore::new(&path).expect("reader");
        let events = reader.load_events(workflow_id).expect("events");
        assert_eq!(count_step_resumed(&events, 2), 0);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn webhook_after_completion_is_ok_noop() {
        let path = test_db_path("done");
        let _ = std::fs::remove_file(&path);
        let thread_id = "thd_done";
        let workflow_id = "wf-done";
        let engine = seed_waiting_workflow(&path, workflow_id, thread_id).await;

        engine
            .resume(workflow_id, 2, "approved".to_string())
            .await
            .expect("resume");

        let reader = SqliteStore::new(&path).expect("reader");
        for _ in 0..200 {
            let events = reader.load_events(workflow_id).expect("events");
            if workflow_completed(&events) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        let mut state = ApiState::new_with_webhook_secret(&path, TEST_SECRET);
        state.engine = engine;

        let body = format!(
            r#"{{"event_type":"message.received","message":{{"thread_id":"{thread_id}","text":"approved again"}}}}"#,
        );
        let svix_id = "svix_after_done";
        let ts = now_timestamp();
        let sig = sign_payload(TEST_SECRET, svix_id, &ts, body.as_bytes());

        let app = router().with_state(state);
        let response = app
            .oneshot(webhook_request(&body, svix_id, &ts, &sig))
            .await
            .expect("oneshot");
        assert_eq!(response.status(), StatusCode::OK);

        let events = reader.load_events(workflow_id).expect("events");
        assert_eq!(count_step_resumed(&events, 2), 1);
        let _ = std::fs::remove_file(&path);
    }
}
