use anyhow::{Context, Result, bail};
use std::collections::HashMap;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;
use walkdir::WalkDir;

use crate::events::{RemoteChange, RemoteFileMeta};
use crate::hash_utils::{compute_md5, modified_rfc3339};

use super::RemoteStore;

pub struct FsRemoteStore {
    sandbox_root: PathBuf,
    trash_root: PathBuf,
    snapshot_hashes: Mutex<HashMap<String, String>>,
}

impl FsRemoteStore {
    pub fn new(sandbox_root: PathBuf, trash_root: PathBuf) -> Result<Self> {
        fs::create_dir_all(&sandbox_root)
            .with_context(|| format!("failed to create {}", sandbox_root.display()))?;
        fs::create_dir_all(&trash_root)
            .with_context(|| format!("failed to create {}", trash_root.display()))?;

        Ok(Self {
            sandbox_root,
            trash_root,
            snapshot_hashes: Mutex::new(HashMap::new()),
        })
    }

    fn absolute_from_virtual(&self, virtual_path: &str) -> Result<PathBuf> {
        let relative = sanitize_virtual_path(virtual_path)?;
        Ok(self.sandbox_root.join(relative))
    }

    fn metadata_for_virtual(&self, virtual_path: &str) -> Result<Option<RemoteFileMeta>> {
        let normalized = normalize_remote_path(virtual_path);
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

    fn scan_files(&self) -> Result<HashMap<String, RemoteFileMeta>> {
        let mut out = HashMap::new();

        for entry in WalkDir::new(&self.sandbox_root)
            .into_iter()
            .filter_map(std::result::Result::ok)
        {
            if !entry.file_type().is_file() {
                continue;
            }

            let absolute_path = entry.path();
            let relative = absolute_path
                .strip_prefix(&self.sandbox_root)
                .with_context(|| {
                    format!(
                        "failed to strip {} from {}",
                        self.sandbox_root.display(),
                        absolute_path.display()
                    )
                })?;

            let virtual_path = normalize_remote_path(&relative.to_string_lossy());
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
}

impl RemoteStore for FsRemoteStore {
    fn ensure_sandbox(&self) -> Result<()> {
        fs::create_dir_all(&self.sandbox_root)
            .with_context(|| format!("failed to create {}", self.sandbox_root.display()))?;
        Ok(())
    }

    fn upload_or_update(&self, virtual_path: &str, local_path: &Path) -> Result<RemoteFileMeta> {
        if !local_path.exists() {
            bail!("local path does not exist: {}", local_path.display());
        }
        if local_path.is_dir() {
            bail!(
                "upload_or_update currently supports files only, got directory {}",
                local_path.display()
            );
        }

        let normalized = normalize_remote_path(virtual_path);
        let target = self.absolute_from_virtual(&normalized)?;

        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }

        fs::copy(local_path, &target).with_context(|| {
            format!(
                "failed to copy {} -> {}",
                local_path.display(),
                target.display()
            )
        })?;

        self.metadata_for_virtual(&normalized)?
            .context("metadata not available after upload")
    }

    fn download_to_local(&self, virtual_path: &str, local_path: &Path) -> Result<RemoteFileMeta> {
        let normalized = normalize_remote_path(virtual_path);
        let source = self.absolute_from_virtual(&normalized)?;

        if !source.exists() {
            bail!("remote file does not exist: {}", normalized);
        }
        if source.is_dir() {
            bail!("download_to_local only supports files, got directory {normalized}");
        }

        if let Some(parent) = local_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }

        fs::copy(&source, local_path).with_context(|| {
            format!(
                "failed to copy {} -> {}",
                source.display(),
                local_path.display()
            )
        })?;

        self.metadata_for_virtual(&normalized)?
            .context("metadata not available after download")
    }

    fn rename(&self, from_virtual_path: &str, to_virtual_path: &str) -> Result<RemoteFileMeta> {
        let from_normalized = normalize_remote_path(from_virtual_path);
        let to_normalized = normalize_remote_path(to_virtual_path);

        let from_abs = self.absolute_from_virtual(&from_normalized)?;
        let to_abs = self.absolute_from_virtual(&to_normalized)?;

        if !from_abs.exists() {
            bail!("remote source does not exist: {}", from_normalized);
        }

        if let Some(parent) = to_abs.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }

        fs::rename(&from_abs, &to_abs).with_context(|| {
            format!(
                "failed to rename {} -> {}",
                from_abs.display(),
                to_abs.display()
            )
        })?;

        self.metadata_for_virtual(&to_normalized)?
            .context("metadata not available after rename")
    }

    fn trash(&self, virtual_path: &str) -> Result<()> {
        let normalized = normalize_remote_path(virtual_path);
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

        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }

        fs::rename(&source, &target).with_context(|| {
            format!(
                "failed to move remote file to trash {} -> {}",
                source.display(),
                target.display()
            )
        })?;

        Ok(())
    }

    fn poll_changes(&self) -> Result<Vec<RemoteChange>> {
        let current = self.scan_files()?;

        let mut snapshot = self
            .snapshot_hashes
            .lock()
            .map_err(|_| anyhow::anyhow!("remote snapshot mutex poisoned"))?;

        let previous = snapshot.clone();
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

        *snapshot = current
            .iter()
            .map(|(path, meta)| (path.clone(), meta.md5_hash.clone()))
            .collect();

        Ok(changes)
    }

    fn get_metadata(&self, virtual_path: &str) -> Result<Option<RemoteFileMeta>> {
        self.metadata_for_virtual(virtual_path)
    }
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

fn normalize_remote_path(raw: &str) -> String {
    raw.trim_matches('/')
        .replace('\\', "/")
        .split('/')
        .filter(|segment| !segment.is_empty() && *segment != ".")
        .collect::<Vec<_>>()
        .join("/")
}

fn make_drive_id(virtual_path: &str) -> String {
    Uuid::new_v5(&Uuid::NAMESPACE_URL, virtual_path.as_bytes()).to_string()
}
