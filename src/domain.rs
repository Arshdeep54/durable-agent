#![allow(dead_code)]

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Ticket {
    pub id: String,
    pub customer_id: String,
    pub subject: String,
    pub body: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Classification {
    pub category: String,
    pub urgency: String,
    pub draft_reply: String,
    /// Set by the real LLM classifier so traces can show model and token usage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub llm: Option<LlmCall>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LlmCall {
    pub model: String,
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub latency_ms: u64,
    pub input: String,
}
