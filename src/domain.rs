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
}
