use std::sync::Mutex;

pub struct ApprovalCorrelations {
    conn: Mutex<rusqlite::Connection>,
}

impl ApprovalCorrelations {
    pub fn new(path: &str) -> rusqlite::Result<Self> {
        let conn = rusqlite::Connection::open(path)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS approval_correlations (
                correlation_key TEXT PRIMARY KEY,
                workflow_id TEXT NOT NULL,
                step_index INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS processed_webhooks (
                svix_id TEXT PRIMARY KEY
            );",
        )?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    pub fn insert(
        &self,
        correlation_key: &str,
        workflow_id: &str,
        step_index: usize,
    ) -> rusqlite::Result<()> {
        let conn = self.conn.lock().expect("correlations connection");
        conn.execute(
            "INSERT INTO approval_correlations (correlation_key, workflow_id, step_index)
             VALUES (?1, ?2, ?3)",
            rusqlite::params![correlation_key, workflow_id, step_index as i64],
        )?;
        Ok(())
    }

    pub fn lookup(&self, correlation_key: &str) -> rusqlite::Result<Option<(String, usize)>> {
        let conn = self.conn.lock().expect("correlations connection");
        let mut stmt = conn.prepare(
            "SELECT workflow_id, step_index FROM approval_correlations WHERE correlation_key = ?1",
        )?;
        let mut rows = stmt.query([correlation_key])?;
        match rows.next()? {
            Some(row) => {
                let workflow_id: String = row.get(0)?;
                let step_index: i64 = row.get(1)?;
                Ok(Some((workflow_id, step_index as usize)))
            }
            None => Ok(None),
        }
    }

    pub fn is_webhook_processed(&self, svix_id: &str) -> rusqlite::Result<bool> {
        let conn = self.conn.lock().expect("correlations connection");
        let mut stmt = conn.prepare("SELECT 1 FROM processed_webhooks WHERE svix_id = ?1")?;
        let mut rows = stmt.query([svix_id])?;
        Ok(rows.next()?.is_some())
    }

    pub fn mark_webhook_processed(&self, svix_id: &str) -> rusqlite::Result<()> {
        let conn = self.conn.lock().expect("correlations connection");
        conn.execute(
            "INSERT INTO processed_webhooks (svix_id) VALUES (?1)",
            [svix_id],
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_db_path(suffix: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "durable-agent-correlations-test-{}-{}.db",
            std::process::id(),
            suffix
        ))
    }

    #[test]
    fn correlation_round_trip() {
        let path = test_db_path("round_trip");
        let _ = std::fs::remove_file(&path);
        let store = ApprovalCorrelations::new(path.to_str().expect("path")).expect("new");
        store.insert("thd_1", "wf-1", 2).expect("insert");
        let got = store.lookup("thd_1").expect("lookup").expect("some");
        assert_eq!(got, ("wf-1".to_string(), 2));
        assert!(store.lookup("missing").expect("lookup").is_none());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn processed_webhook_idempotency_flag() {
        let path = test_db_path("processed");
        let _ = std::fs::remove_file(&path);
        let store = ApprovalCorrelations::new(path.to_str().expect("path")).expect("new");
        assert!(!store.is_webhook_processed("msg_1").expect("check"));
        store.mark_webhook_processed("msg_1").expect("mark");
        assert!(store.is_webhook_processed("msg_1").expect("check"));
        let _ = std::fs::remove_file(&path);
    }
}
