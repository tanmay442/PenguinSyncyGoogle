use anyhow::Result;
use async_trait::async_trait;
use std::path::Path;

use crate::events::{RemoteChange, RemoteFileMeta};

mod fs_remote;
mod google_drive;

pub use fs_remote::FsRemoteStore;
pub use google_drive::GoogleDriveRemoteStore;

#[async_trait]
pub trait RemoteStore: Send + Sync {
    async fn ensure_sandbox(&self) -> Result<()>;

    async fn upload_or_update(
        &self,
        virtual_path: &str,
        local_path: &Path,
    ) -> Result<RemoteFileMeta>;

    async fn download_to_local(
        &self,
        virtual_path: &str,
        local_path: &Path,
    ) -> Result<RemoteFileMeta>;

    async fn rename(&self, from_virtual_path: &str, to_virtual_path: &str)
        -> Result<RemoteFileMeta>;

    async fn trash(&self, virtual_path: &str) -> Result<()>;

    async fn poll_changes(&self) -> Result<Vec<RemoteChange>>;

    async fn get_metadata(&self, virtual_path: &str) -> Result<Option<RemoteFileMeta>>;
}
