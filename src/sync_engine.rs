use anyhow::{Context, Result, anyhow};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;
use tracing::{debug, error, info, warn};
use walkdir::WalkDir;

use crate::config::AppConfig;
use crate::events::{LocalFileEvent, LocalFileEventKind, RemoteChange, SyncEvent};
use crate::hash_utils::{compute_md5, modified_unix_timestamp};
use crate::remote::RemoteStore;
use crate::state_db::{FileState, StateDb};

#[derive(Debug, Clone)]
struct PendingDelete {
    record: FileState,
    expires_at: Instant,
}

pub struct SyncEngine {
    config: AppConfig,
    db: StateDb,
    remote: Arc<dyn RemoteStore>,
    rx: mpsc::Receiver<SyncEvent>,
    pending_deletes: HashMap<PathBuf, PendingDelete>,
}

impl SyncEngine {
    pub fn new(
        config: AppConfig,
        db: StateDb,
        remote: Arc<dyn RemoteStore>,
        rx: mpsc::Receiver<SyncEvent>,
    ) -> Self {
        Self {
            config,
            db,
            remote,
            rx,
            pending_deletes: HashMap::new(),
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        let mut cleanup_tick = tokio::time::interval(Duration::from_secs(1));
        cleanup_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

        info!("sync engine started");

        loop {
            tokio::select! {
                _ = cleanup_tick.tick() => {
                    if let Err(err) = self.flush_expired_pending_deletes().await {
                        error!("failed to flush pending deletes: {err:#}");
                    }
                }
                maybe = self.rx.recv() => {
                    match maybe {
                        Some(SyncEvent::Shutdown) => {
                            info!("sync engine received shutdown event");
                            self.flush_all_pending_deletes().await?;
                            break;
                        }
                        Some(event) => {
                            if let Err(err) = self.handle_event(event).await {
                                error!("sync event failed: {err:#}");
                            }
                        }
                        None => {
                            info!("sync event channel closed");
                            self.flush_all_pending_deletes().await?;
                            break;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn handle_event(&mut self, event: SyncEvent) -> Result<()> {
        match event {
            SyncEvent::Local(local_event) => self.handle_local_event(local_event).await,
            SyncEvent::Remote(remote_event) => self.handle_remote_event(remote_event).await,
            SyncEvent::SyncNow => self.sync_now().await,
            SyncEvent::Shutdown => Ok(()),
        }
    }

    async fn handle_local_event(&mut self, event: LocalFileEvent) -> Result<()> {
        match event.kind {
            LocalFileEventKind::Upsert => self.handle_local_upsert(event.path).await,
            LocalFileEventKind::Delete => self.handle_local_delete(event.path),
        }
    }

    async fn handle_remote_event(&mut self, event: RemoteChange) -> Result<()> {
        match event {
            RemoteChange::Upsert(meta) => self.handle_remote_upsert(meta.virtual_path).await,
            RemoteChange::Delete { virtual_path } => self.handle_remote_delete(virtual_path),
        }
    }

    async fn handle_local_upsert(&mut self, path: PathBuf) -> Result<()> {
        if !path.exists() {
            debug!("upsert ignored for missing path {}", path.display());
            return Ok(());
        }
        if path.is_dir() {
            return Ok(());
        }

        self.pending_deletes.remove(&path);

        let Some(virtual_path) = self.config.local_to_remote(&path) else {
            debug!("path not covered by config, skipping: {}", path.display());
            return Ok(());
        };

        let local_hash = compute_md5(&path)?;
        let local_timestamp = modified_unix_timestamp(&path)?;

        let existing = self.db.get_by_local_path(&path)?;

        if existing.is_none()
            && let Some((old_local_path, pending_delete)) =
                self.find_pending_delete_with_hash(&local_hash, &path)
        {
            let renamed_meta = match self
                .remote
                .rename(&pending_delete.record.virtual_remote_path, &virtual_path)
                .await
            {
                Ok(meta) => meta,
                Err(err) => {
                    warn!(
                        "remote rename failed ({} -> {}), falling back to upload: {err:#}",
                        pending_delete.record.virtual_remote_path, virtual_path
                    );
                    self.remote.upload_or_update(&virtual_path, &path).await?
                }
            };

            self.db.remove_by_local_path(&old_local_path)?;
            self.pending_deletes.remove(&old_local_path);

            self.db.upsert(&FileState {
                local_path: path.clone(),
                drive_id: renamed_meta.drive_id,
                virtual_remote_path: virtual_path,
                md5_hash: local_hash,
                local_timestamp,
                remote_timestamp: renamed_meta.modified_time,
            })?;

            info!(
                "rename detected using hash match: {} -> {}",
                old_local_path.display(),
                path.display()
            );
            return Ok(());
        }

        if let Some(current) = existing
            && current.md5_hash == local_hash
        {
            self.db.update_local_timestamp(&path, local_timestamp)?;
            debug!("content unchanged, timestamp refreshed: {}", path.display());
            return Ok(());
        }

        let uploaded = self.remote.upload_or_update(&virtual_path, &path).await?;

        self.db.upsert(&FileState {
            local_path: path.clone(),
            drive_id: uploaded.drive_id,
            virtual_remote_path: uploaded.virtual_path,
            md5_hash: local_hash,
            local_timestamp,
            remote_timestamp: uploaded.modified_time,
        })?;

        info!("uploaded {}", path.display());
        Ok(())
    }

    fn handle_local_delete(&mut self, path: PathBuf) -> Result<()> {
        let records = self.db.list_by_local_prefix(&path)?;
        if records.is_empty() {
            debug!("delete ignored, no db record for {}", path.display());
            return Ok(());
        }

        let expires_at = Instant::now() + Duration::from_secs(5);

        for record in records {
            self.pending_deletes.insert(
                record.local_path.clone(),
                PendingDelete { record, expires_at },
            );
        }

        info!(
            "scheduled {} pending delete(s) rooted at {}",
            self.pending_deletes.len(),
            path.display()
        );

        Ok(())
    }

    async fn handle_remote_upsert(&mut self, virtual_path: String) -> Result<()> {
        let Some(local_path) = self.config.remote_to_local(&virtual_path) else {
            debug!("remote path not mapped to local watch: {}", virtual_path);
            return Ok(());
        };

        let Some(remote_meta) = self.remote.get_metadata(&virtual_path).await? else {
            warn!("remote upsert event had no metadata: {}", virtual_path);
            return Ok(());
        };

        let tracked = self.db.get_by_remote_path(&virtual_path)?;

        if let Some(record) = &tracked
            && record.md5_hash == remote_meta.md5_hash
        {
            debug!("ignoring remote echo for {}", virtual_path);
            return Ok(());
        }

        let local_hash = if local_path.exists() && local_path.is_file() {
            Some(compute_md5(&local_path)?)
        } else {
            None
        };

        if let (Some(record), Some(local_hash)) = (tracked.clone(), local_hash.clone())
            && local_hash != record.md5_hash
            && remote_meta.md5_hash != record.md5_hash
        {
            let conflicted_path = make_conflicted_copy_path(&local_path)?;
            self.remote
                .download_to_local(&virtual_path, &conflicted_path)
                .await
                .with_context(|| {
                    format!(
                        "failed to store conflicted copy for {} at {}",
                        virtual_path,
                        conflicted_path.display()
                    )
                })?;

            warn!(
                "conflict detected for {}; remote copied to {}",
                local_path.display(),
                conflicted_path.display()
            );

            let uploaded = self.remote.upload_or_update(&virtual_path, &local_path).await?;
            let local_timestamp = modified_unix_timestamp(&local_path)?;

            self.db.upsert(&FileState {
                local_path,
                drive_id: uploaded.drive_id,
                virtual_remote_path: uploaded.virtual_path,
                md5_hash: local_hash,
                local_timestamp,
                remote_timestamp: uploaded.modified_time,
            })?;

            return Ok(());
        }

        let downloaded = self.remote.download_to_local(&virtual_path, &local_path).await?;
        let local_timestamp = modified_unix_timestamp(&local_path)?;

        self.db.upsert(&FileState {
            local_path: local_path.clone(),
            drive_id: downloaded.drive_id,
            virtual_remote_path: downloaded.virtual_path,
            md5_hash: downloaded.md5_hash,
            local_timestamp,
            remote_timestamp: downloaded.modified_time,
        })?;

        info!(
            "downloaded remote change {} -> {}",
            virtual_path,
            local_path.display()
        );

        Ok(())
    }

    fn handle_remote_delete(&mut self, virtual_path: String) -> Result<()> {
        let records = self.db.list_by_remote_prefix(&virtual_path)?;

        if records.is_empty() {
            if let Some(local_path) = self.config.remote_to_local(&virtual_path)
                && local_path.exists()
                && let Err(err) = trash::delete(&local_path)
            {
                warn!(
                    "failed to move local file to desktop trash {}: {err}",
                    local_path.display()
                );
            }
            self.db.remove_by_remote_path(&virtual_path)?;
            return Ok(());
        }

        for record in records {
            if record.local_path.exists()
                && let Err(err) = trash::delete(&record.local_path)
            {
                warn!(
                    "failed to move local file to desktop trash {}: {err}",
                    record.local_path.display()
                );
            }

            self.db.remove_by_local_path(&record.local_path)?;
        }

        info!("handled remote delete for {}", virtual_path);

        Ok(())
    }

    async fn sync_now(&mut self) -> Result<()> {
        info!("manual sync started");

        let watch_entries = self.config.watch.clone();

        for watch in watch_entries {
            if !watch.local_path.exists() {
                continue;
            }

            if watch.local_path.is_file() {
                self.handle_local_upsert(watch.local_path.clone()).await?;
                continue;
            }

            for entry in WalkDir::new(&watch.local_path)
                .into_iter()
                .filter_map(std::result::Result::ok)
            {
                if entry.file_type().is_file() {
                    self.handle_local_upsert(entry.path().to_path_buf()).await?;
                }
            }
        }

        for record in self.db.list_all()? {
            if !record.local_path.exists() {
                self.pending_deletes.insert(
                    record.local_path.clone(),
                    PendingDelete {
                        record,
                        expires_at: Instant::now(),
                    },
                );
            }
        }

        self.flush_expired_pending_deletes().await?;

        info!("manual sync finished");
        Ok(())
    }

    async fn flush_expired_pending_deletes(&mut self) -> Result<()> {
        let now = Instant::now();

        let expired = self
            .pending_deletes
            .iter()
            .filter_map(|(path, pending)| (pending.expires_at <= now).then_some(path.clone()))
            .collect::<Vec<_>>();

        for path in expired {
            let Some(pending) = self.pending_deletes.remove(&path) else {
                continue;
            };

            self.remote.trash(&pending.record.virtual_remote_path).await?;
            self.db.remove_by_local_path(&pending.record.local_path)?;

            info!(
                "soft-deleted remote copy {} (local {})",
                pending.record.virtual_remote_path,
                pending.record.local_path.display()
            );
        }

        Ok(())
    }

    async fn flush_all_pending_deletes(&mut self) -> Result<()> {
        for pending in self.pending_deletes.values_mut() {
            pending.expires_at = Instant::now();
        }
        self.flush_expired_pending_deletes().await
    }

    fn find_pending_delete_with_hash(
        &self,
        hash: &str,
        new_local_path: &Path,
    ) -> Option<(PathBuf, PendingDelete)> {
        self.pending_deletes
            .iter()
            .find_map(|(old_local_path, pending)| {
                if old_local_path == new_local_path {
                    return None;
                }
                (pending.record.md5_hash == hash)
                    .then_some((old_local_path.clone(), pending.clone()))
            })
    }
}

fn make_conflicted_copy_path(original: &Path) -> Result<PathBuf> {
    let parent = original
        .parent()
        .ok_or_else(|| anyhow!("path has no parent: {}", original.display()))?;

    let stem = original
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("file");

    let ext = original.extension().and_then(|value| value.to_str());

    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| anyhow!("system time is before UNIX_EPOCH"))?
        .as_millis();

    let mut file_name = format!("{stem} (conflicted copy {millis})");
    if let Some(ext) = ext {
        file_name.push('.');
        file_name.push_str(ext);
    }

    Ok(parent.join(file_name))
}
