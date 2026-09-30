use std::time::Duration;

use agentq::{Backoff, RetryPolicy, StepDef, Workflow};

pub fn ticket_workflow(ticket_id: &str) -> Workflow {
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
                timeout: None,
            },
            StepDef {
                name: "RequestApproval".to_string(),
                retry_policy: cheap.clone(),
                timeout: None,
            },
            StepDef {
                name: "SendReply".to_string(),
                retry_policy: external.clone(),
                timeout: None,
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
