#![allow(dead_code)]

use crate::domain::Ticket;
use std::future::Future;
use std::pin::Pin;

#[derive(Debug)]
pub struct ApprovalError(pub String);

impl std::fmt::Display for ApprovalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for ApprovalError {}

pub trait ApprovalSender: Send + Sync {
    fn request_approval(
        &self,
        ticket: &Ticket,
        draft_reply: &str,
    ) -> Pin<Box<dyn Future<Output = Result<(), ApprovalError>> + Send>>;
}

#[derive(Debug, Default)]
pub struct MockApprovalSender;

impl ApprovalSender for MockApprovalSender {
    fn request_approval(
        &self,
        ticket: &Ticket,
        draft_reply: &str,
    ) -> Pin<Box<dyn Future<Output = Result<(), ApprovalError>> + Send>> {
        let id = ticket.id.clone();
        let draft = draft_reply.to_string();
        Box::pin(async move {
            println!("approval request for ticket {id}: {draft}");
            Ok(())
        })
    }
}

pub struct AgentMailSender {
    api_key: String,
    from_address: String,
    to_address: String,
    client: reqwest::Client,
}

impl AgentMailSender {
    pub fn new(api_key: String, from_address: String, to_address: String) -> Self {
        Self {
            api_key,
            from_address,
            to_address,
            client: reqwest::Client::new(),
        }
    }

    pub async fn send(&self, to: &str, subject: &str, text: &str) -> Result<(), ApprovalError> {
        let inbox_id = self.from_address.replace('@', "%40");
        let url = format!(
            "https://api.agentmail.to/v0/inboxes/{inbox_id}/messages/send"
        );

        let response = self
            .client
            .post(&url)
            .bearer_auth(&self.api_key)
            .json(&serde_json::json!({
                "to": to,
                "subject": subject,
                "text": text,
            }))
            .send()
            .await
            .map_err(|e| ApprovalError(e.to_string()))?;

        let status = response.status();
        if status.is_success() {
            Ok(())
        } else {
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| String::new());
            Err(ApprovalError(format!(
                "agentmail send failed with {status}: {body}"
            )))
        }
    }
}

impl ApprovalSender for AgentMailSender {
    fn request_approval(
        &self,
        ticket: &Ticket,
        draft_reply: &str,
    ) -> Pin<Box<dyn Future<Output = Result<(), ApprovalError>> + Send>> {
        let ticket_id = ticket.id.clone();
        let ticket_subject = ticket.subject.clone();
        let draft = draft_reply.to_string();
        let to_address = self.to_address.clone();
        let sender = AgentMailSender {
            api_key: self.api_key.clone(),
            from_address: self.from_address.clone(),
            to_address: self.to_address.clone(),
            client: self.client.clone(),
        };

        Box::pin(async move {
            let subject = format!("Approval needed: ticket {ticket_id}");
            let text = format!(
                "Subject: {ticket_subject}\n\nDraft reply:\n{draft}"
            );
            sender.send(&to_address, &subject, &text).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::Ticket;

    fn sample_ticket() -> Ticket {
        Ticket {
            id: "t1".into(),
            customer_id: "c1".into(),
            subject: "help".into(),
            body: "need assistance".into(),
        }
    }

    #[tokio::test]
    async fn mock_approval_sender_returns_ok() {
        let sender = MockApprovalSender;
        let ticket = sample_ticket();
        let result = sender
            .request_approval(&ticket, "Thanks for reaching out.")
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    #[ignore = "requires AGENTMAIL_API_KEY, AGENTMAIL_FROM_ADDRESS, APPROVER_EMAIL and sends real email"]
    async fn agentmail_sender_sends_approval_request() {
        let sender = AgentMailSender::new(
            std::env::var("AGENTMAIL_API_KEY").expect("AGENTMAIL_API_KEY"),
            std::env::var("AGENTMAIL_FROM_ADDRESS").expect("AGENTMAIL_FROM_ADDRESS"),
            std::env::var("APPROVER_EMAIL").expect("APPROVER_EMAIL"),
        );
        let ticket = sample_ticket();
        let result = sender
            .request_approval(&ticket, "Thanks for reaching out — we're on it.")
            .await;
        assert!(result.is_ok());
    }
}
