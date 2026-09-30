#![allow(dead_code)]

use crate::domain::Ticket;
use std::future::Future;
use std::pin::Pin;

#[derive(Debug)]
pub struct ApprovalError {
    pub message: String,
    pub status: Option<u16>,
}

impl std::fmt::Display for ApprovalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.message.fmt(f)
    }
}

impl std::error::Error for ApprovalError {}

pub trait CustomerMailer: Send + Sync {
    fn send_reply(
        &self,
        to: &str,
        subject: &str,
        text: &str,
    ) -> Pin<Box<dyn Future<Output = Result<(), ApprovalError>> + Send>>;
}

#[derive(Debug, Default)]
pub struct MockCustomerMailer;

impl CustomerMailer for MockCustomerMailer {
    fn send_reply(
        &self,
        to: &str,
        subject: &str,
        text: &str,
    ) -> Pin<Box<dyn Future<Output = Result<(), ApprovalError>> + Send>> {
        let to = to.to_string();
        let subject = subject.to_string();
        let text = text.to_string();
        Box::pin(async move {
            tracing::info!(
                to = %to,
                subject = %subject,
                text = %text,
                "mock customer reply sent"
            );
            Ok(())
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
pub struct SentMessage {
    pub message_id: String,
    pub thread_id: String,
}

pub trait ApprovalSender: Send + Sync {
    fn request_approval(
        &self,
        ticket: &Ticket,
        draft_reply: &str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<String>, ApprovalError>> + Send>>;
}

#[derive(Debug, Default)]
pub struct MockApprovalSender;

impl ApprovalSender for MockApprovalSender {
    fn request_approval(
        &self,
        ticket: &Ticket,
        draft_reply: &str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<String>, ApprovalError>> + Send>> {
        let id = ticket.id.clone();
        let draft = draft_reply.to_string();
        Box::pin(async move {
            tracing::info!(workflow_id = %id, draft = %draft, "mock approval request sent");
            Ok(None)
        })
    }
}

#[derive(Clone)]
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

    pub async fn send(
        &self,
        to: &str,
        subject: &str,
        text: &str,
    ) -> Result<SentMessage, ApprovalError> {
        let inbox_id = self.from_address.replace('@', "%40");
        let url = format!("https://api.agentmail.to/v0/inboxes/{inbox_id}/messages/send");

        tracing::info!(to = %to, subject = %subject, "agentmail send");
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
            .map_err(|e| {
                tracing::error!(to = %to, subject = %subject, error = %e, "agentmail send failed");
                ApprovalError {
                    message: e.to_string(),
                    status: None,
                }
            })?;

        let status = response.status();
        if status.is_success() {
            let body = response.text().await.map_err(|e| ApprovalError {
                message: e.to_string(),
                status: None,
            })?;
            let sent: SentMessage = serde_json::from_str(&body).map_err(|e| ApprovalError {
                message: format!("agentmail send response parse failed: {e}: {body}"),
                status: None,
            })?;
            Ok(sent)
        } else {
            let body = response.text().await.unwrap_or_else(|_| String::new());
            tracing::error!(
                to = %to,
                subject = %subject,
                status = %status,
                body = %body,
                "agentmail send failed"
            );
            Err(ApprovalError {
                message: format!("agentmail send failed with {status}: {body}"),
                status: Some(status.as_u16()),
            })
        }
    }
}

impl CustomerMailer for AgentMailSender {
    fn send_reply(
        &self,
        to: &str,
        subject: &str,
        text: &str,
    ) -> Pin<Box<dyn Future<Output = Result<(), ApprovalError>> + Send>> {
        let this = self.clone();
        let to = to.to_string();
        let subject = subject.to_string();
        let text = text.to_string();
        Box::pin(async move { this.send(&to, &subject, &text).await.map(|_| ()) })
    }
}

impl ApprovalSender for AgentMailSender {
    fn request_approval(
        &self,
        ticket: &Ticket,
        draft_reply: &str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<String>, ApprovalError>> + Send>> {
        let ticket_id = ticket.id.clone();
        let ticket_subject = ticket.subject.clone();
        let draft = draft_reply.to_string();
        let to_address = self.to_address.clone();
        let sender = self.clone();

        Box::pin(async move {
            let subject = format!("Approval needed: ticket {ticket_id}");
            let text = format!("Subject: {ticket_subject}\n\nDraft reply:\n{draft}");
            let sent = sender.send(&to_address, &subject, &text).await?;
            Ok(Some(sent.thread_id))
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
