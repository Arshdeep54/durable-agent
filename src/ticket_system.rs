#![allow(dead_code)]

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;

#[derive(Debug)]
pub struct TicketSystemError(pub String);

impl std::fmt::Display for TicketSystemError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for TicketSystemError {}

pub trait TicketSystem: Send + Sync {
    fn mark_resolved(
        &self,
        ticket_id: &str,
    ) -> Pin<Box<dyn Future<Output = Result<(), TicketSystemError>> + Send>>;
}

pub struct InMemoryTicketSystem {
    resolved: Mutex<HashSet<String>>,
}

impl InMemoryTicketSystem {
    pub fn new() -> Self {
        Self {
            resolved: Mutex::new(HashSet::new()),
        }
    }

    pub fn is_resolved(&self, ticket_id: &str) -> bool {
        self.resolved
            .lock()
            .expect("resolved set lock")
            .contains(ticket_id)
    }
}

impl TicketSystem for InMemoryTicketSystem {
    fn mark_resolved(
        &self,
        ticket_id: &str,
    ) -> Pin<Box<dyn Future<Output = Result<(), TicketSystemError>> + Send>> {
        let id = ticket_id.to_string();
        let result = match self.resolved.lock() {
            Ok(mut guard) => {
                guard.insert(id);
                Ok(())
            }
            Err(e) => Err(TicketSystemError(e.to_string())),
        };
        Box::pin(async move { result })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn mark_resolved_once_updates_set() {
        let system = InMemoryTicketSystem::new();
        system
            .mark_resolved("ticket-42")
            .await
            .expect("first mark should succeed");
        assert!(system.is_resolved("ticket-42"));
    }

    #[tokio::test]
    async fn mark_resolved_twice_is_idempotent() {
        let system = InMemoryTicketSystem::new();
        system
            .mark_resolved("ticket-42")
            .await
            .expect("first mark should succeed");
        system
            .mark_resolved("ticket-42")
            .await
            .expect("second mark should not error");
        assert!(system.is_resolved("ticket-42"));
    }
}
