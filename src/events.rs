use std::path::PathBuf;

#[derive(Debug, Clone)]
pub enum SyncEvent {
    Local(LocalFileEvent),
    Remote(RemoteChange),
    SyncNow,
    Shutdown,
}

#[derive(Debug, Clone)]
pub struct LocalFileEvent {
    pub kind: LocalFileEventKind,
    pub path: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalFileEventKind {
    Upsert,
    Delete,
}

#[derive(Debug, Clone)]
pub enum RemoteChange {
    Upsert(RemoteFileMeta),
    Delete { virtual_path: String },
}

#[derive(Debug, Clone)]
pub struct RemoteFileMeta {
    pub drive_id: String,
    pub virtual_path: String,
    pub md5_hash: String,
    pub modified_time: String,
}
