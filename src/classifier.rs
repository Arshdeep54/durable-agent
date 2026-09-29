#![allow(dead_code)]

use crate::domain::{Classification, Ticket};
use async_openai::Client;
use async_openai::config::OpenAIConfig;
use async_openai::types::chat::{
    ChatCompletionRequestMessage, ChatCompletionRequestSystemMessage,
    ChatCompletionRequestUserMessage, CreateChatCompletionRequest,
};
use std::future::Future;
use std::pin::Pin;

#[derive(Debug)]
pub struct ClassifyError(pub String);

impl std::fmt::Display for ClassifyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for ClassifyError {}

pub trait Classifier: Send + Sync {
    fn classify(
        &self,
        ticket: &Ticket,
    ) -> Pin<Box<dyn Future<Output = Result<Classification, ClassifyError>> + Send>>;
}

#[derive(Debug, Default)]
pub struct MockClassifier;

fn mock_classify(body: &str) -> Classification {
    let lower = body.to_lowercase();
    let (category, urgency) = if lower.contains("refund") || lower.contains("charge") {
        ("billing", "high")
    } else if lower.contains("password") || lower.contains("login") {
        ("account", "high")
    } else {
        ("general", "normal")
    };

    Classification {
        category: category.to_string(),
        urgency: urgency.to_string(),
        draft_reply: format!(
            "Thanks for reaching out about your {category} issue — we're on it."
        ),
    }
}

impl Classifier for MockClassifier {
    fn classify(
        &self,
        ticket: &Ticket,
    ) -> Pin<Box<dyn Future<Output = Result<Classification, ClassifyError>> + Send>> {
        let body = ticket.body.clone();
        Box::pin(async move { Ok(mock_classify(&body)) })
    }
}

pub struct OpenAiClassifier {
    client: Client<OpenAIConfig>,
}

impl OpenAiClassifier {
    pub fn new() -> Self {
        Self {
            client: Client::new(),
        }
    }
}

const SYSTEM_PROMPT: &str = r"You classify customer support tickets. Reply with a single JSON object only, no markdown, with keys: category (string), urgency (string), draft_reply (string).";

impl Classifier for OpenAiClassifier {
    fn classify(
        &self,
        ticket: &Ticket,
    ) -> Pin<Box<dyn Future<Output = Result<Classification, ClassifyError>> + Send>> {
        let client = self.client.clone();
        let subject = ticket.subject.clone();
        let body = ticket.body.clone();

        Box::pin(async move {
            let user_content = format!("Subject: {subject}\n\n{body}");
            let request = CreateChatCompletionRequest {
                model: "gpt-4o-mini".into(),
                messages: vec![
                    ChatCompletionRequestMessage::System(
                        ChatCompletionRequestSystemMessage::from(SYSTEM_PROMPT),
                    ),
                    ChatCompletionRequestMessage::User(
                        ChatCompletionRequestUserMessage::from(user_content),
                    ),
                ],
                ..Default::default()
            };

            let response = client
                .chat()
                .create(request)
                .await
                .map_err(|e| ClassifyError(e.to_string()))?;

            let content = response
                .choices
                .first()
                .and_then(|c| c.message.content.clone())
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(|| ClassifyError("empty model response".into()))?;

            serde_json::from_str::<Classification>(&content).map_err(|e| {
                ClassifyError(format!("failed to parse classification JSON: {e}"))
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::Ticket;

    fn sample_ticket(body: &str) -> Ticket {
        Ticket {
            id: "t1".into(),
            customer_id: "c1".into(),
            subject: "help".into(),
            body: body.into(),
        }
    }

    async fn run_classify(classifier: &dyn Classifier, ticket: &Ticket) -> Classification {
        classifier
            .classify(ticket)
            .await
            .expect("classify should succeed")
    }

    #[tokio::test]
    async fn mock_classifier_billing_keywords() {
        let c = MockClassifier;
        let result = run_classify(&c, &sample_ticket("I need a refund on my last charge")).await;
        assert_eq!(result.category, "billing");
        assert_eq!(result.urgency, "high");
        assert!(result.draft_reply.contains("billing"));
    }

    #[tokio::test]
    async fn mock_classifier_account_keywords() {
        let c = MockClassifier;
        let result = run_classify(&c, &sample_ticket("Can't reset my password or login")).await;
        assert_eq!(result.category, "account");
        assert_eq!(result.urgency, "high");
        assert!(result.draft_reply.contains("account"));
    }

    #[tokio::test]
    async fn mock_classifier_general_fallback() {
        let c = MockClassifier;
        let result = run_classify(&c, &sample_ticket("Just saying hello")).await;
        assert_eq!(result.category, "general");
        assert_eq!(result.urgency, "normal");
        assert!(result.draft_reply.contains("general"));
    }

    #[tokio::test]
    #[ignore = "requires OPENAI_API_KEY and network"]
    async fn openai_classifier_returns_parsed_classification() {
        let classifier = OpenAiClassifier::new();
        let ticket = sample_ticket("My card was charged twice this month.");
        let result = classifier.classify(&ticket).await.expect("openai classify");

        assert!(!result.category.is_empty());
        assert!(!result.urgency.is_empty());
        assert!(!result.draft_reply.is_empty());
    }
}
