use crate::events::{RemoteChange, RemoteFileMeta};
use crate::hash_utils::{compute_md5, modified_rfc3339};
use crate::remote::RemoteStore;
use anyhow::{Context, Result, bail};
use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::task;
use uuid::Uuid;
use walkdir::WalkDir;

pub struct FsRemoteStore {
    sandbox_root: PathBuf,
    trash_root: PathBuf,
    snapshot_hashes: std::sync::Mutex<HashMap<String, String>>,
}

impl FsRemoteStore {
    pub fn new(sandbox_root: PathBuf, trash_root: PathBuf) -> Result<Self> {
        Ok(Self {
            sandbox_root,
            trash_root,
            snapshot_hashes: std::sync::Mutex::new(HashMap::new()),
        })
    }

    fn absolute_from_virtual(&self, virtual_path: &str) -> Result<PathBuf> {
        let relative = sanitize_virtual_path(virtual_path)?;
        Ok(self.sandbox_root.join(relative))
    }

    fn scan_files_sync(&self) -> Result<HashMap<String, RemoteFileMeta>> {
        let mut out = HashMap::new();
        let sandbox = self.sandbox_root.clone();

        for entry in WalkDir::new(&sandbox)
            .into_iter()
            .filter_map(std::result::Result::ok)
        {
            if !entry.file_type().is_file() {
                continue;
            }

            let absolute_path = entry.path();
            let relative = absolute_path
                .strip_prefix(&sandbox)
                .context("failed to strip sandbox prefix")?;

            let virtual_path = normalize_virtual_path(&relative.to_string_lossy());
            let md5_hash = compute_md5(absolute_path)?;
            let modified_time = modified_rfc3339(absolute_path)?;

            out.insert(
                virtual_path.clone(),
                RemoteFileMeta {
                    drive_id: make_drive_id(&virtual_path),
                    virtual_path,
                    md5_hash,
                    modified_time,
                },
            );
        }

        Ok(out)
    }

    fn metadata_sync(&self, virtual_path: &str) -> Result<Option<RemoteFileMeta>> {
        let normalized = normalize_virtual_path(virtual_path);
        let absolute_path = self.absolute_from_virtual(&normalized)?;

        if !absolute_path.exists() || absolute_path.is_dir() {
            return Ok(None);
        }

        let md5_hash = compute_md5(&absolute_path)?;
        let modified_time = modified_rfc3339(&absolute_path)?;

        Ok(Some(RemoteFileMeta {
            drive_id: make_drive_id(&normalized),
            virtual_path: normalized,
            md5_hash,
            modified_time,
        }))
    }
}

#[async_trait::async_trait]
impl RemoteStore for FsRemoteStore {
    async fn ensure_sandbox(&self) -> Result<()> {
        let sandbox = self.sandbox_root.clone();
        task::spawn_blocking(move || {
            std::fs::create_dir_all(&sandbox)
                .with_context(|| format!("failed to create sandbox {}", sandbox.display()))
        })
        .await?
    }

    async fn upload_or_update(
        &self,
        virtual_path: &str,
        local_path: &Path,
    ) -> Result<RemoteFileMeta> {
        if !local_path.exists() {
            bail!("local path does not exist: {}", local_path.display());
        }
        if local_path.is_dir() {
            bail!(
                "upload_or_update currently supports files only, got directory {}",
                local_path.display()
            );
        }

        let normalized = normalize_virtual_path(virtual_path);
        let target = self.absolute_from_virtual(&normalized)?;
        let sandbox = self.sandbox_root.clone();

        let local = local_path.to_path_buf();
        let target_copy = target.clone();

        task::spawn_blocking(move || {
            if let Some(parent) = target_copy.parent() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("failed to create {}", parent.display()))?;
            }
            std::fs::copy(&local, &target_copy).with_context(|| {
                format!(
                    "failed to copy {} -> {}",
                    local.display(),
                    target_copy.display()
                )
            })?;
            Ok::<(), anyhow::Error>(())
        })
        .await??;

        self.metadata_sync(&normalized)?
            .context("metadata not available after upload")
    }

    async fn download_to_local(
        &self,
        virtual_path: &str,
        local_path: &Path,
    ) -> Result<RemoteFileMeta> {
        let normalized = normalize_virtual_path(virtual_path);
        let source = self.absolute_from_virtual(&normalized)?;

        if !source.exists() {
            bail!("remote file does not exist: {normalized}");
        }
        if source.is_dir() {
            bail!("download_to_local only supports files, got directory {normalized}");
        }

        let dest = local_path.to_path_buf();
        let source_copy = source.clone();

        task::spawn_blocking(move || {
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("failed to create {}", parent.display()))?;
            }
            std::fs::copy(&source_copy, &dest).with_context(|| {
                format!(
                    "failed to copy {} -> {}",
                    source_copy.display(),
                    dest.display()
                )
            })?;
            Ok::<(), anyhow::Error>(())
        })
        .await??;

        self.metadata_sync(&normalized)?
            .context("metadata not available after download")
    }

    async fn rename(
        &self,
        from_virtual_path: &str,
        to_virtual_path: &str,
    ) -> Result<RemoteFileMeta> {
        let from_normalized = normalize_virtual_path(from_virtual_path);
        let to_normalized = normalize_virtual_path(to_virtual_path);

        let from_abs = self.absolute_from_virtual(&from_normalized)?;
        let to_abs = self.absolute_from_virtual(&to_normalized)?;

        if !from_abs.exists() {
            bail!("remote source does not exist: {from_normalized}");
        }

        let from_clone = from_abs.clone();
        let to_clone = to_abs.clone();

        task::spawn_blocking(move || {
            if let Some(parent) = to_clone.parent() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("failed to create {}", parent.display()))?;
            }
            std::fs::rename(&from_clone, &to_clone).with_context(|| {
                format!("failed to rename {} -> {}", from_clone.display(), to_clone.display())
            })?;
            Ok::<(), anyhow::Error>(())
        })
        .await??;

        self.metadata_sync(&to_normalized)?
            .context("metadata not available after rename")
    }

    async fn trash(&self, virtual_path: &str) -> Result<()> {
        let normalized = normalize_virtual_path(virtual_path);
        let source = self.absolute_from_virtual(&normalized)?;

        if !source.exists() {
            return Ok(());
        }

        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system time is before UNIX_EPOCH")?
            .as_millis();

        let safe_name = normalized.replace('/', "__");
        let target = self.trash_root.join(format!("{ts}_{safe_name}"));

        let source_clone = source.clone();
        let target_clone = target.clone();

        task::spawn_blocking(move || {
            if let Some(parent) = target_clone.parent() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("failed to create {}", parent.display()))?;
            }
            std::fs::rename(&source_clone, &target_clone).with_context(|| {
                format!(
                    "failed to move remote file to trash {} -> {}",
                    source_clone.display(),
                    target_clone.display()
                )
            })?;
            Ok::<(), anyhow::Error>(())
        })
        .await?
    }

    async fn poll_changes(&self) -> Result<Vec<RemoteChange>> {
        let sandbox = self.sandbox_root.clone();
        let snapshot = Arc::new(std::sync::Mutex::new(
            self.snapshot_hashes.lock().unwrap().clone()
        ));
        let snapshot_ref = Arc::clone(&snapshot);

        let current = task::spawn_blocking(move || {
            let mut out = HashMap::new();
            for entry in WalkDir::new(&sandbox)
                .into_iter()
                .filter_map(std::result::Result::ok)
            {
                if !entry.file_type().is_file() {
                    continue;
                }
                let absolute_path = entry.path();
                let relative = absolute_path.strip_prefix(&sandbox)
                    .context("failed to strip sandbox prefix")?;
                let virtual_path = normalize_virtual_path(&relative.to_string_lossy());
                let md5_hash = compute_md5(absolute_path)?;
                let modified_time = modified_rfc3339(absolute_path)?;
                out.insert(virtual_path.clone(), RemoteFileMeta {
                    drive_id: make_drive_id(&virtual_path),
                    virtual_path,
                    md5_hash,
                    modified_time,
                });
            }
            Ok::<_, anyhow::Error>(out)
        }).await??;

        let previous = snapshot_ref.lock().unwrap().clone();
        let mut changes = Vec::new();

        for (path, meta) in &current {
            match previous.get(path) {
                Some(old_hash) if old_hash == &meta.md5_hash => {}
                _ => changes.push(RemoteChange::Upsert(meta.clone())),
            }
        }

        for old_path in previous.keys() {
            if !current.contains_key(old_path) {
                changes.push(RemoteChange::Delete {
                    virtual_path: old_path.clone(),
                });
            }
        }

        let updates: HashMap<String, String> = current
            .iter()
            .map(|(p, m)| (p.clone(), m.md5_hash.clone()))
            .collect();

        *self.snapshot_hashes.lock().unwrap() = updates;
        Ok(changes)
    }

async fn get_metadata(&self, virtual_path: &str) -> Result<Option<RemoteFileMeta>> {
        let normalized = normalize_virtual_path(virtual_path);
        let sandbox = self.sandbox_root.clone();

        task::spawn_blocking(move || {
            for entry in WalkDir::new(&sandbox)
                .into_iter()
                .filter_map(std::result::Result::ok)
            {
                if !entry.file_type().is_file() {
                    continue;
                }
                let absolute_path = entry.path();
                let relative = absolute_path.strip_prefix(&sandbox)
                    .context("failed to strip sandbox prefix")?;
                let vp = normalize_virtual_path(&relative.to_string_lossy());
                if vp == normalized {
                    let md5_hash = compute_md5(absolute_path)?;
                    let modified_time = modified_rfc3339(absolute_path)?;
                    return Ok(Some(RemoteFileMeta {
                        drive_id: make_drive_id(&vp),
                        virtual_path: vp,
                        md5_hash,
                        modified_time,
                    }));
                }
            }
            Ok(None)
        }).await?
    }
}

fn normalize_virtual_path(raw: &str) -> String {
    raw.trim()
        .trim_matches('/')
        .replace('\\', "/")
        .split('/')
        .filter(|segment| !segment.is_empty() && *segment != ".")
        .collect::<Vec<_>>()
        .join("/")
}

fn sanitize_virtual_path(raw: &str) -> Result<PathBuf> {
    let mut out = PathBuf::new();
    for component in Path::new(raw).components() {
        match component {
            Component::Normal(segment) => out.push(segment),
            Component::CurDir => {}
            _ => bail!("invalid remote path component in {raw}"),
        }
    }
    if out.as_os_str().is_empty() {
        bail!("remote path cannot be empty");
    }
    Ok(out)
}

fn make_drive_id(virtual_path: &str) -> String {
    Uuid::new_v5(&Uuid::NAMESPACE_URL, virtual_path.as_bytes()).to_string()
}