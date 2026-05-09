use anyhow::Result;
use std::path::Path;

use crate::events::{RemoteChange, RemoteFileMeta};

mod fs_remote;

pub use fs_remote::FsRemoteStore;

pub trait RemoteStore: Send + Sync {
    fn ensure_sandbox(&self) -> Result<()>;

    fn upload_or_update(&self, virtual_path: &str, local_path: &Path) -> Result<RemoteFileMeta>;

    fn download_to_local(&self, virtual_path: &str, local_path: &Path) -> Result<RemoteFileMeta>;

    fn rename(&self, from_virtual_path: &str, to_virtual_path: &str) -> Result<RemoteFileMeta>;

    fn trash(&self, virtual_path: &str) -> Result<()>;

    fn poll_changes(&self) -> Result<Vec<RemoteChange>>;

    fn get_metadata(&self, virtual_path: &str) -> Result<Option<RemoteFileMeta>>;
}
