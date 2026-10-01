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

pub struct SqliteTicketSystem {
    conn: Mutex<rusqlite::Connection>,
}

impl SqliteTicketSystem {
    pub fn new(path: &str) -> rusqlite::Result<Self> {
        let conn = rusqlite::Connection::open(path)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS resolved_tickets (
                ticket_id TEXT PRIMARY KEY
            );",
        )?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    pub fn is_resolved(&self, ticket_id: &str) -> bool {
        let conn = self.conn.lock().expect("ticket system connection");
        let mut stmt = conn
            .prepare("SELECT 1 FROM resolved_tickets WHERE ticket_id = ?1")
            .expect("prepare is_resolved");
        let mut rows = stmt.query([ticket_id]).expect("query is_resolved");
        rows.next().expect("next is_resolved").is_some()
    }
}

impl TicketSystem for SqliteTicketSystem {
    fn mark_resolved(
        &self,
        ticket_id: &str,
    ) -> Pin<Box<dyn Future<Output = Result<(), TicketSystemError>> + Send>> {
        let id = ticket_id.to_string();
        let result = match self.conn.lock() {
            Ok(conn) => conn
                .execute(
                    "INSERT OR IGNORE INTO resolved_tickets (ticket_id) VALUES (?1)",
                    [&id],
                )
                .map(|_| ())
                .map_err(|e| TicketSystemError(e.to_string())),
            Err(e) => Err(TicketSystemError(e.to_string())),
        };
        Box::pin(async move { result })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_db_path(suffix: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "durable-agent-ticket-system-test-{}-{}.db",
            std::process::id(),
            suffix
        ))
    }

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

    #[tokio::test]
    async fn sqlite_mark_resolved_survives_restart() {
        let path = test_db_path("restart");
        let _ = std::fs::remove_file(&path);
        let path_str = path.to_str().expect("path");
        let system = SqliteTicketSystem::new(path_str).expect("new");
        system.mark_resolved("ticket-restart").await.expect("mark");
        assert!(system.is_resolved("ticket-restart"));
        drop(system);
        let system = SqliteTicketSystem::new(path_str).expect("reopen");
        assert!(system.is_resolved("ticket-restart"));
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn sqlite_mark_resolved_twice_is_idempotent() {
        let path = test_db_path("idempotent");
        let _ = std::fs::remove_file(&path);
        let system = SqliteTicketSystem::new(path.to_str().expect("path")).expect("new");
        system
            .mark_resolved("ticket-42")
            .await
            .expect("first mark should succeed");
        system
            .mark_resolved("ticket-42")
            .await
            .expect("second mark should not error");
        assert!(system.is_resolved("ticket-42"));
        let _ = std::fs::remove_file(&path);
    }
}
