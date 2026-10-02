use std::sync::Arc;

use agentq::{DurableStore, Event};
use reqwest::Client;
use serde::Serialize;

pub struct TraceSpan {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub path: String,
    pub name: String,
    pub log_type: &'static str,
    pub output: String,
    pub metadata: serde_json::Value,
}

pub trait TraceSink: Send + Sync {
    fn record_batch(&self, spans: Vec<TraceSpan>);
}

pub struct NoopSink;

impl TraceSink for NoopSink {
    fn record_batch(&self, _spans: Vec<TraceSpan>) {}
}

const RESPAN_INGEST_URL: &str = "https://api.respan.ai/api/v1/traces/ingest";

pub struct RespanSink {
    api_key: String,
    ingest_url: String,
    client: Client,
}

impl RespanSink {
    pub fn new(api_key: String) -> Self {
        Self::with_ingest_url(api_key, RESPAN_INGEST_URL.to_string())
    }

    pub fn with_ingest_url(api_key: String, ingest_url: String) -> Self {
        Self {
            api_key,
            ingest_url,
            client: Client::new(),
        }
    }
}

#[derive(Serialize)]
struct RespanIngestSpan {
    trace_unique_id: String,
    span_unique_id: String,
    span_parent_id: Option<String>,
    span_path: String,
    span_name: String,
    log_type: &'static str,
    output: String,
    metadata: serde_json::Value,
}

impl TraceSink for RespanSink {
    fn record_batch(&self, spans: Vec<TraceSpan>) {
        if spans.is_empty() {
            return;
        }
        let api_key = self.api_key.clone();
        let ingest_url = self.ingest_url.clone();
        let client = self.client.clone();
        let payload: Vec<RespanIngestSpan> = spans
            .into_iter()
            .map(|span| RespanIngestSpan {
                trace_unique_id: span.trace_id,
                span_unique_id: span.span_id,
                span_parent_id: span.parent_span_id,
                span_path: span.path,
                span_name: span.name,
                log_type: span.log_type,
                output: span.output,
                metadata: span.metadata,
            })
            .collect();
        tokio::spawn(async move {
            let response = client
                .post(&ingest_url)
                .header("Authorization", format!("Bearer {api_key}"))
                .json(&payload)
                .send()
                .await;
            match response {
                Ok(resp) => {
                    if !resp.status().is_success() {
                        let status = resp.status();
                        let body = resp.text().await.unwrap_or_default();
                        tracing::error!(
                            status = %status,
                            body = %body,
                            "respan ingest failed"
                        );
                    }
                }
                Err(e) => {
                    tracing::error!(error = %e, "respan ingest failed");
                }
            }
        });
    }
}

fn workflow_id_from_events(events: &[Event]) -> Option<String> {
    events.first().map(|event| match event {
        Event::WorkflowStarted { workflow_id } => workflow_id.clone(),
        Event::StepStarted { workflow_id, .. } => workflow_id.clone(),
        Event::StepCompleted { workflow_id, .. } => workflow_id.clone(),
        Event::StepFailed { workflow_id, .. } => workflow_id.clone(),
        Event::RetryScheduled { workflow_id, .. } => workflow_id.clone(),
        Event::StepWaiting { workflow_id, .. } => workflow_id.clone(),
        Event::StepResumed { workflow_id, .. } => workflow_id.clone(),
        Event::WorkflowCompleted { workflow_id } => workflow_id.clone(),
        Event::WorkflowFailed { workflow_id, .. } => workflow_id.clone(),
        Event::WorkflowCancelled { workflow_id, .. } => workflow_id.clone(),
        Event::WorkerRecovered { workflow_id, .. } => workflow_id.clone(),
    })
}

fn event_kind(event: &Event) -> &'static str {
    match event {
        Event::WorkflowStarted { .. } => "WorkflowStarted",
        Event::StepStarted { .. } => "StepStarted",
        Event::StepCompleted { .. } => "StepCompleted",
        Event::StepFailed { .. } => "StepFailed",
        Event::RetryScheduled { .. } => "RetryScheduled",
        Event::StepWaiting { .. } => "StepWaiting",
        Event::StepResumed { .. } => "StepResumed",
        Event::WorkflowCompleted { .. } => "WorkflowCompleted",
        Event::WorkflowFailed { .. } => "WorkflowFailed",
        Event::WorkflowCancelled { .. } => "WorkflowCancelled",
        Event::WorkerRecovered { .. } => "WorkerRecovered",
    }
}

fn event_metadata(event: &Event, worker_id: &str) -> serde_json::Value {
    let kind = event_kind(event);
    match event {
        Event::WorkflowStarted { workflow_id } => {
            serde_json::json!({
                "workflow_id": workflow_id,
                "worker_id": worker_id,
                "event_kind": kind,
            })
        }
        Event::StepStarted {
            workflow_id,
            step_index,
            attempt,
        } => {
            serde_json::json!({
                "workflow_id": workflow_id,
                "worker_id": worker_id,
                "event_kind": kind,
                "step_index": step_index,
                "attempt": attempt,
            })
        }
        Event::StepCompleted {
            workflow_id,
            step_index,
            output,
        } => {
            serde_json::json!({
                "workflow_id": workflow_id,
                "worker_id": worker_id,
                "event_kind": kind,
                "step_index": step_index,
                "output": output,
            })
        }
        Event::StepFailed {
            workflow_id,
            step_index,
            reason,
        } => {
            serde_json::json!({
                "workflow_id": workflow_id,
                "worker_id": worker_id,
                "event_kind": kind,
                "step_index": step_index,
                "reason": reason,
            })
        }
        Event::RetryScheduled {
            workflow_id,
            step_index,
            attempt,
        } => {
            serde_json::json!({
                "workflow_id": workflow_id,
                "worker_id": worker_id,
                "event_kind": kind,
                "step_index": step_index,
                "attempt": attempt,
            })
        }
        Event::StepWaiting {
            workflow_id,
            step_index,
            reason,
        } => {
            serde_json::json!({
                "workflow_id": workflow_id,
                "worker_id": worker_id,
                "event_kind": kind,
                "step_index": step_index,
                "reason": reason,
            })
        }
        Event::StepResumed {
            workflow_id,
            step_index,
            input,
        } => {
            serde_json::json!({
                "workflow_id": workflow_id,
                "worker_id": worker_id,
                "event_kind": kind,
                "step_index": step_index,
                "input": input,
            })
        }
        Event::WorkflowCompleted { workflow_id } => {
            serde_json::json!({
                "workflow_id": workflow_id,
                "worker_id": worker_id,
                "event_kind": kind,
            })
        }
        Event::WorkflowFailed {
            workflow_id,
            reason,
        } => {
            serde_json::json!({
                "workflow_id": workflow_id,
                "worker_id": worker_id,
                "event_kind": kind,
                "reason": reason,
            })
        }
        Event::WorkflowCancelled {
            workflow_id,
            reason,
        } => {
            serde_json::json!({
                "workflow_id": workflow_id,
                "worker_id": worker_id,
                "event_kind": kind,
                "reason": reason,
            })
        }
        Event::WorkerRecovered {
            workflow_id,
            step_index,
        } => {
            serde_json::json!({
                "workflow_id": workflow_id,
                "worker_id": worker_id,
                "event_kind": kind,
                "step_index": step_index,
            })
        }
    }
}

fn event_step_index(event: &Event) -> Option<usize> {
    match event {
        Event::StepStarted { step_index, .. }
        | Event::StepCompleted { step_index, .. }
        | Event::StepFailed { step_index, .. }
        | Event::RetryScheduled { step_index, .. }
        | Event::StepWaiting { step_index, .. }
        | Event::StepResumed { step_index, .. }
        | Event::WorkerRecovered { step_index, .. } => Some(*step_index),
        Event::WorkflowStarted { .. }
        | Event::WorkflowCompleted { .. }
        | Event::WorkflowFailed { .. }
        | Event::WorkflowCancelled { .. } => None,
    }
}

fn event_span_name(event: &Event, kind: &str) -> String {
    match event_step_index(event) {
        Some(step_index) => format!("{kind} (step {step_index})"),
        None => kind.to_string(),
    }
}

pub fn events_to_trace_spans(events: &[Event], worker_id: &str) -> Vec<TraceSpan> {
    if events.is_empty() {
        return Vec::new();
    }
    let workflow_id = workflow_id_from_events(events).unwrap_or_default();
    if workflow_id.is_empty() {
        return Vec::new();
    }
    let root_span_id = format!("{workflow_id}:root");
    let root_metadata = serde_json::json!({
        "workflow_id": workflow_id,
        "worker_id": worker_id,
        "event_kind": "workflow_root",
        "event_count": events.len(),
    });
    let mut spans = vec![TraceSpan {
        trace_id: workflow_id.clone(),
        span_id: root_span_id.clone(),
        parent_span_id: None,
        path: "workflow/root".to_string(),
        name: workflow_id.clone(),
        log_type: "workflow",
        output: root_metadata.to_string(),
        metadata: root_metadata,
    }];
    for (index, event) in events.iter().enumerate() {
        let kind = event_kind(event);
        let metadata = event_metadata(event, worker_id);
        spans.push(TraceSpan {
            trace_id: workflow_id.clone(),
            span_id: format!("{workflow_id}:{index}"),
            parent_span_id: Some(root_span_id.clone()),
            path: format!("workflow/{kind}"),
            name: event_span_name(event, kind),
            log_type: "task",
            output: metadata.to_string(),
            metadata,
        });
    }
    spans
}

pub fn spawn_workflow_trace_batch(
    reader_store: Arc<agentq::SqliteStore>,
    sink: Arc<dyn TraceSink>,
    worker_id: String,
    workflow_id: String,
) {
    tokio::spawn(async move {
        let events = match reader_store.load_events(&workflow_id) {
            Ok(events) => events,
            Err(e) => {
                tracing::error!(
                    workflow_id = %workflow_id,
                    error = %e,
                    "trace forward: load_events failed"
                );
                return;
            }
        };
        let spans = events_to_trace_spans(&events, &worker_id);
        sink.record_batch(spans);
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noop_sink_record_batch_does_not_panic() {
        let sink = NoopSink;
        sink.record_batch(vec![]);
        sink.record_batch(vec![TraceSpan {
            trace_id: "w".into(),
            span_id: "w:0".into(),
            parent_span_id: Some("w:root".into()),
            path: "workflow/WorkflowStarted".into(),
            name: "WorkflowStarted".into(),
            log_type: "task",
            output: "{}".into(),
            metadata: serde_json::json!({}),
        }]);
    }

    #[test]
    fn events_to_trace_spans_maps_root_and_children() {
        let events = vec![
            Event::WorkflowStarted {
                workflow_id: "wf-1".into(),
            },
            Event::StepStarted {
                workflow_id: "wf-1".into(),
                step_index: 0,
                attempt: 0,
            },
            Event::StepWaiting {
                workflow_id: "wf-1".into(),
                step_index: 2,
                reason: "approval".into(),
            },
        ];
        let spans = events_to_trace_spans(&events, "worker-1");
        assert_eq!(spans.len(), 4);

        let root = &spans[0];
        assert_eq!(root.trace_id, "wf-1");
        assert_eq!(root.span_id, "wf-1:root");
        assert!(root.parent_span_id.is_none());
        assert_eq!(root.path, "workflow/root");
        assert_eq!(root.name, "wf-1");
        assert_eq!(root.log_type, "workflow");
        assert!(!root.output.is_empty());
        assert_eq!(root.metadata["workflow_id"], "wf-1");
        assert_eq!(root.metadata["worker_id"], "worker-1");
        assert_eq!(root.metadata["event_kind"], "workflow_root");

        let started = &spans[1];
        assert_eq!(started.span_id, "wf-1:0");
        assert_eq!(started.parent_span_id.as_deref(), Some("wf-1:root"));
        assert_eq!(started.path, "workflow/WorkflowStarted");
        assert_eq!(started.name, "WorkflowStarted");
        assert_eq!(started.log_type, "task");
        assert_eq!(started.metadata["event_kind"], "WorkflowStarted");
        assert_eq!(started.metadata["workflow_id"], "wf-1");

        let step_started = &spans[2];
        assert_eq!(step_started.path, "workflow/StepStarted");
        assert_eq!(step_started.name, "StepStarted (step 0)");
        assert_eq!(step_started.metadata["step_index"], 0);
        assert_eq!(step_started.metadata["attempt"], 0);

        let waiting = &spans[3];
        assert_eq!(waiting.path, "workflow/StepWaiting");
        assert_eq!(waiting.name, "StepWaiting (step 2)");
        assert_eq!(waiting.metadata["step_index"], 2);
        assert_eq!(waiting.metadata["reason"], "approval");
    }

    #[test]
    fn events_to_trace_spans_includes_step_resumed_input() {
        let events = vec![
            Event::WorkflowStarted {
                workflow_id: "wf-2".into(),
            },
            Event::StepResumed {
                workflow_id: "wf-2".into(),
                step_index: 1,
                input: "approved".into(),
            },
            Event::WorkerRecovered {
                workflow_id: "wf-2".into(),
                step_index: 0,
            },
        ];
        let spans = events_to_trace_spans(&events, "worker-2");
        assert_eq!(spans.len(), 4);

        let resumed = &spans[2];
        assert_eq!(resumed.path, "workflow/StepResumed");
        assert_eq!(resumed.metadata["input"], "approved");

        let recovered = &spans[3];
        assert_eq!(recovered.path, "workflow/WorkerRecovered");
        assert_eq!(recovered.name, "WorkerRecovered (step 0)");
    }
}
