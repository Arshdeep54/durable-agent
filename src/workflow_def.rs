use std::time::Duration;

use agentq::{Backoff, RetryPolicy, StepDef, Workflow};

fn step_timeout_from_env(var: &str, default_secs: u64) -> Option<Duration> {
    Some(Duration::from_secs(
        std::env::var(var)
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(default_secs),
    ))
}

pub fn ticket_workflow(ticket_id: &str) -> Workflow {
    let classify_timeout = step_timeout_from_env("CLASSIFY_TIMEOUT_SECS", 30);
    let send_reply_timeout = step_timeout_from_env("SEND_REPLY_TIMEOUT_SECS", 15);
    let cheap = RetryPolicy {
        max_attempts: 1,
        backoff: Backoff::Fixed(Duration::from_millis(1)),
    };
    let external = RetryPolicy {
        max_attempts: 3,
        backoff: Backoff::Exponential {
            base: Duration::from_millis(200),
            max: Duration::from_secs(5),
        },
    };

    Workflow {
        id: ticket_id.to_string(),
        steps: vec![
            StepDef {
                name: "IngestTicket".to_string(),
                retry_policy: cheap.clone(),
                timeout: None,
            },
            StepDef {
                name: "ClassifyTicket".to_string(),
                retry_policy: external.clone(),
                timeout: classify_timeout,
            },
            StepDef {
                name: "RequestApproval".to_string(),
                retry_policy: cheap.clone(),
                timeout: None,
            },
            StepDef {
                name: "SendReply".to_string(),
                retry_policy: external.clone(),
                timeout: send_reply_timeout,
            },
            StepDef {
                name: "UpdateTicketSystem".to_string(),
                retry_policy: cheap.clone(),
                timeout: None,
            },
            StepDef {
                name: "Complete".to_string(),
                retry_policy: cheap,
                timeout: None,
            },
        ],
    }
}
