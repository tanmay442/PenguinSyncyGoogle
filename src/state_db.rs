use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, Row, params};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct FileState {
    pub local_path: PathBuf,
    pub drive_id: String,
    pub virtual_remote_path: String,
    pub md5_hash: String,
    pub local_timestamp: i64,
    pub remote_timestamp: String,
}

pub struct StateDb {
    conn: Connection,
}

impl StateDb {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }

        let conn = Connection::open(path)
            .with_context(|| format!("failed to open sqlite db at {}", path.display()))?;

        let db = Self { conn };
        db.init_schema()?;
        Ok(db)
    }

    fn init_schema(&self) -> Result<()> {
        self.conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS file_state (
                local_path TEXT PRIMARY KEY,
                drive_id TEXT NOT NULL,
                virtual_remote_path TEXT NOT NULL,
                md5_hash TEXT NOT NULL,
                local_timestamp INTEGER NOT NULL,
                remote_timestamp TEXT NOT NULL
            );

            CREATE UNIQUE INDEX IF NOT EXISTS idx_file_state_remote
            ON file_state(virtual_remote_path);

            CREATE INDEX IF NOT EXISTS idx_file_state_md5
            ON file_state(md5_hash);
            "#,
        )?;

        Ok(())
    }

    pub fn upsert(&self, record: &FileState) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO file_state (
                local_path,
                drive_id,
                virtual_remote_path,
                md5_hash,
                local_timestamp,
                remote_timestamp
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            ON CONFLICT(local_path) DO UPDATE SET
                drive_id = excluded.drive_id,
                virtual_remote_path = excluded.virtual_remote_path,
                md5_hash = excluded.md5_hash,
                local_timestamp = excluded.local_timestamp,
                remote_timestamp = excluded.remote_timestamp
            "#,
            params![
                normalize_local_path(&record.local_path),
                record.drive_id,
                normalize_remote_path(&record.virtual_remote_path),
                record.md5_hash,
                record.local_timestamp,
                record.remote_timestamp,
            ],
        )?;

        Ok(())
    }

    pub fn get_by_local_path(&self, local_path: &Path) -> Result<Option<FileState>> {
        let mut stmt = self.conn.prepare(
            "SELECT local_path, drive_id, virtual_remote_path, md5_hash, local_timestamp, remote_timestamp
             FROM file_state
             WHERE local_path = ?1",
        )?;

        let row = stmt
            .query_row(params![normalize_local_path(local_path)], row_to_file_state)
            .optional()?;

        Ok(row)
    }

    pub fn get_by_remote_path(&self, virtual_remote_path: &str) -> Result<Option<FileState>> {
        let mut stmt = self.conn.prepare(
            "SELECT local_path, drive_id, virtual_remote_path, md5_hash, local_timestamp, remote_timestamp
             FROM file_state
             WHERE virtual_remote_path = ?1",
        )?;

        let row = stmt
            .query_row(
                params![normalize_remote_path(virtual_remote_path)],
                row_to_file_state,
            )
            .optional()?;

        Ok(row)
    }

    pub fn list_all(&self) -> Result<Vec<FileState>> {
        let mut stmt = self.conn.prepare(
            "SELECT local_path, drive_id, virtual_remote_path, md5_hash, local_timestamp, remote_timestamp
             FROM file_state",
        )?;

        let rows = stmt
            .query_map([], row_to_file_state)?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(rows)
    }

    pub fn list_by_local_prefix(&self, prefix: &Path) -> Result<Vec<FileState>> {
        let normalized = normalize_local_path(prefix);
        let like = format!("{}/%", normalized.trim_end_matches('/'));

        let mut stmt = self.conn.prepare(
            "SELECT local_path, drive_id, virtual_remote_path, md5_hash, local_timestamp, remote_timestamp
             FROM file_state
             WHERE local_path = ?1 OR local_path LIKE ?2",
        )?;

        let rows = stmt
            .query_map(params![normalized, like], row_to_file_state)?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(rows)
    }

    pub fn list_by_remote_prefix(&self, prefix: &str) -> Result<Vec<FileState>> {
        let normalized = normalize_remote_path(prefix);
        let like = format!("{}/%", normalized.trim_end_matches('/'));

        let mut stmt = self.conn.prepare(
            "SELECT local_path, drive_id, virtual_remote_path, md5_hash, local_timestamp, remote_timestamp
             FROM file_state
             WHERE virtual_remote_path = ?1 OR virtual_remote_path LIKE ?2",
        )?;

        let rows = stmt
            .query_map(params![normalized, like], row_to_file_state)?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(rows)
    }

    pub fn remove_by_local_path(&self, local_path: &Path) -> Result<()> {
        self.conn.execute(
            "DELETE FROM file_state WHERE local_path = ?1",
            params![normalize_local_path(local_path)],
        )?;
        Ok(())
    }

    pub fn remove_by_remote_path(&self, remote_path: &str) -> Result<()> {
        self.conn.execute(
            "DELETE FROM file_state WHERE virtual_remote_path = ?1",
            params![normalize_remote_path(remote_path)],
        )?;
        Ok(())
    }

    pub fn update_local_timestamp(&self, local_path: &Path, local_timestamp: i64) -> Result<()> {
        self.conn.execute(
            "UPDATE file_state SET local_timestamp = ?2 WHERE local_path = ?1",
            params![normalize_local_path(local_path), local_timestamp],
        )?;

        Ok(())
    }
}

fn row_to_file_state(row: &Row<'_>) -> rusqlite::Result<FileState> {
    Ok(FileState {
        local_path: PathBuf::from(row.get::<_, String>(0)?),
        drive_id: row.get(1)?,
        virtual_remote_path: row.get(2)?,
        md5_hash: row.get(3)?,
        local_timestamp: row.get(4)?,
        remote_timestamp: row.get(5)?,
    })
}

fn normalize_local_path(path: &Path) -> String {
    path.to_string_lossy().to_string()
}

fn normalize_remote_path(path: &str) -> String {
    path.trim_matches('/').replace('\\', "/")
}
