#![allow(dead_code)]

use crate::domain::Ticket;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct WorkflowRegistry {
    conn: Mutex<rusqlite::Connection>,
}

impl WorkflowRegistry {
    pub fn new(path: &str) -> rusqlite::Result<Self> {
        let conn = rusqlite::Connection::open(path)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS workflows (
                id TEXT PRIMARY KEY,
                ticket_json TEXT NOT NULL,
                created_at INTEGER NOT NULL
            );",
        )?;
        Ok(Self { conn: Mutex::new(conn) })
    }

    pub fn insert(&self, ticket: &Ticket) -> rusqlite::Result<()> {
        let ticket_json = serde_json::to_string(ticket).map_err(|e| {
            rusqlite::Error::ToSqlConversionFailure(Box::new(e))
        })?;
        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?
            .as_secs() as i64;
        let conn = self.conn.lock().expect("registry connection");
        conn.execute(
            "INSERT INTO workflows (id, ticket_json, created_at) VALUES (?1, ?2, ?3)",
            rusqlite::params![&ticket.id, ticket_json, created_at],
        )?;
        Ok(())
    }

    pub fn get(&self, id: &str) -> rusqlite::Result<Option<Ticket>> {
        let conn = self.conn.lock().expect("registry connection");
        let mut stmt = conn.prepare("SELECT ticket_json FROM workflows WHERE id = ?1")?;
        let mut rows = stmt.query([id])?;
        match rows.next()? {
            Some(row) => {
                let json: String = row.get(0)?;
                let ticket = serde_json::from_str(&json).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
                })?;
                Ok(Some(ticket))
            }
            None => Ok(None),
        }
    }

    pub fn list(&self) -> rusqlite::Result<Vec<String>> {
        let conn = self.conn.lock().expect("registry connection");
        let mut stmt = conn.prepare("SELECT id FROM workflows ORDER BY created_at")?;
        let mut rows = stmt.query([])?;
        let mut ids = Vec::new();
        while let Some(row) = rows.next()? {
            ids.push(row.get(0)?);
        }
        Ok(ids)
    }
}

fn test_db_path(suffix: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "durable-agent-registry-test-{}-{}.db",
        std::process::id(),
        suffix
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::Ticket;

    fn sample_ticket(id: &str) -> Ticket {
        Ticket {
            id: id.to_string(),
            customer_id: format!("cust-{}", id),
            subject: format!("subject {}", id),
            body: format!("body {}", id),
        }
    }

    #[test]
    fn insert_then_get_round_trip() {
        let path = test_db_path("round_trip");
        let _ = std::fs::remove_file(&path);
        let registry = WorkflowRegistry::new(path.to_str().expect("path")).expect("new");
        let ticket = sample_ticket("t-1");
        registry.insert(&ticket).expect("insert");
        let got = registry.get("t-1").expect("get").expect("some");
        assert_eq!(got.id, ticket.id);
        assert_eq!(got.customer_id, ticket.customer_id);
        assert_eq!(got.subject, ticket.subject);
        assert_eq!(got.body, ticket.body);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn get_unknown_returns_none() {
        let path = test_db_path("unknown");
        let _ = std::fs::remove_file(&path);
        let registry = WorkflowRegistry::new(path.to_str().expect("path")).expect("new");
        assert!(registry.get("missing").expect("get").is_none());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn list_returns_all_ids_ordered_by_created_at() {
        let path = test_db_path("list");
        let _ = std::fs::remove_file(&path);
        let registry = WorkflowRegistry::new(path.to_str().expect("path")).expect("new");
        registry.insert(&sample_ticket("a")).expect("insert");
        registry.insert(&sample_ticket("b")).expect("insert");
        registry.insert(&sample_ticket("c")).expect("insert");
        let ids = registry.list().expect("list");
        assert_eq!(ids, vec!["a".to_string(), "b".to_string(), "c".to_string()]);
        let _ = std::fs::remove_file(&path);
    }
}
