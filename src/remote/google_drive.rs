use anyhow::{Context, Result, bail};
use google_drive3::api::{Change, File};
use google_drive3::DriveHub;
use hyper::body::Bytes;
use http_body_util::BodyExt;
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use mime::Mime;
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::sync::Mutex;
use tokio::sync::Mutex as AsyncMutex;
use yup_oauth2::{InstalledFlowAuthenticator, InstalledFlowReturnMethod, CustomHyperClientBuilder};

use crate::events::{RemoteChange, RemoteFileMeta};

use super::RemoteStore;

const DRIVE_SCOPE: &str = "https://www.googleapis.com/auth/drive";
const FOLDER_MIME: &str = "application/vnd.google-apps.folder";

const FILE_FIELDS: &str = "id,name,parents,mimeType,md5Checksum,modifiedTime,trashed";
const CHANGE_FIELDS: &str = "changes(fileId,removed,file(id,name,parents,mimeType,md5Checksum,modifiedTime,trashed)),nextPageToken,newStartPageToken";

type DriveHubClient = DriveHub<hyper_rustls::HttpsConnector<HttpConnector>>;

pub struct GoogleDriveRemoteStore {
    hub: AsyncMutex<DriveHubClient>,
    sandbox_name: String,
    sandbox_id: AsyncMutex<Option<String>>,
    page_token: AsyncMutex<Option<String>>,
    path_cache: Mutex<HashMap<String, String>>,
    meta_db_path: Option<std::path::PathBuf>,
}

impl GoogleDriveRemoteStore {
    pub async fn new(
        credentials_file: &Path,
        token_cache_file: &Path,
        sandbox_name: String,
        meta_db_path: Option<std::path::PathBuf>,
    ) -> Result<Self> {
        let secret = yup_oauth2::read_application_secret(credentials_file)
            .await
            .with_context(|| {
                format!(
                    "failed to read oauth credentials at {}",
                    credentials_file.display()
                )
            })?;

        let connector = HttpsConnectorBuilder::new()
            .with_native_roots()?
            .https_only()
            .enable_http2()
            .build();

        let executor = TokioExecutor::new();
        let auth_client = Client::builder(executor.clone())
            .build(connector);

        let auth = InstalledFlowAuthenticator::with_client(
            secret,
            InstalledFlowReturnMethod::HTTPRedirect,
            CustomHyperClientBuilder::from(auth_client),
        )
        .persist_tokens_to_disk(token_cache_file)
        .build()
        .await
        .context("failed to initialize oauth flow")?;

        let client_connector = HttpsConnectorBuilder::new()
            .with_native_roots()?
            .https_or_http()
            .enable_http2()
            .build();

        let client = Client::builder(TokioExecutor::new())
            .build(client_connector);

        let hub = DriveHub::new(client, auth);

        Ok(Self {
            hub: AsyncMutex::new(hub),
            sandbox_name: sandbox_name.trim_matches('/').to_string(),
            sandbox_id: AsyncMutex::new(None),
            page_token: AsyncMutex::new(None),
            path_cache: Mutex::new(HashMap::new()),
            meta_db_path,
        })
    }

    fn normalize_remote_path(raw: &str) -> String {
        raw.trim_matches('/')
            .replace('\\', "/")
            .split('/')
            .filter(|segment| !segment.is_empty() && *segment != ".")
            .collect::<Vec<_>>()
            .join("/")
    }

    fn split_segments(path: &str) -> Vec<String> {
        path.split('/')
            .filter(|segment| !segment.is_empty())
            .map(|segment| segment.to_string())
            .collect()
    }

    async fn ensure_sandbox_id(&self) -> Result<String> {
        if let Some(id) = self.sandbox_id.lock().await.clone() {
            return Ok(id);
        }

        let sandbox_id = {
            let mut hub = self.hub.lock().await;
            let query = format!(
                "name = '{}' and mimeType = '{}' and trashed = false and 'root' in parents",
                self.sandbox_name.replace('\'', "\\'"),
                FOLDER_MIME
            );

            let (_, list) = hub
                .files()
                .list()
                .q(&query)
                .spaces("drive")
                .param("fields", "files(id,name,parents)")
                .add_scope(DRIVE_SCOPE)
                .doit()
                .await
                .context("failed to query drive for sandbox folder")?;

            if let Some(files) = list.files {
                if let Some(file) = files.into_iter().next() {
                    if let Some(id) = file.id {
                        id
                    } else {
                        bail!("sandbox folder missing id in response")
                    }
                } else {
                    let mut req = File::default();
                    req.name = Some(self.sandbox_name.clone());
                    req.mime_type = Some(FOLDER_MIME.to_string());

                    let (_, created) = hub
                        .files()
                        .create(req)
                        .param("fields", "id")
                        .add_scope(DRIVE_SCOPE)
                        .upload(std::io::empty(), "application/octet-stream".parse::<Mime>()?)
                        .await
                        .context("failed to create sandbox folder")?;

                    created
                        .id
                        .context("created sandbox folder missing id")?
                }
            } else {
                let mut req = File::default();
                req.name = Some(self.sandbox_name.clone());
                req.mime_type = Some(FOLDER_MIME.to_string());

                let (_, created) = hub
                    .files()
                    .create(req)
                    .param("fields", "id")
                    .add_scope(DRIVE_SCOPE)
                    .upload(std::io::empty(), "application/octet-stream".parse::<Mime>()?)
                    .await
                    .context("failed to create sandbox folder")?;

                created
                    .id
                    .context("created sandbox folder missing id")?
            }
        };

        *self.sandbox_id.lock().await = Some(sandbox_id.clone());
        Ok(sandbox_id)
    }

    async fn load_page_token(&self) -> Result<Option<String>> {
        let Some(path) = &self.meta_db_path else {
            return Ok(None);
        };
        let db = crate::state_db::StateDb::open(path)?;
        db.get_meta("drive_page_token")
    }

    async fn save_page_token(&self, token: &str) -> Result<()> {
        let Some(path) = &self.meta_db_path else {
            return Ok(());
        };
        let db = crate::state_db::StateDb::open(path)?;
        db.set_meta("drive_page_token", token)
    }

    async fn resolve_or_create_folder(&self, parent_id: &str, name: &str) -> Result<String> {
        let cache_key = format!("{parent_id}/{name}");
        if let Some(id) = self.path_cache.lock().unwrap().get(&cache_key) {
            return Ok(id.clone());
        }

        let mut hub = self.hub.lock().await;
        let query = format!(
            "name = '{}' and mimeType = '{}' and trashed = false and '{}' in parents",
            name.replace('\'', "\\'"),
            FOLDER_MIME,
            parent_id
        );

        let (_, list) = hub
            .files()
            .list()
            .q(&query)
            .spaces("drive")
            .param("fields", "files(id,name,parents)")
            .add_scope(DRIVE_SCOPE)
            .doit()
            .await
            .context("failed to query folder")?;

        let id = if let Some(files) = list.files {
            if let Some(file) = files.into_iter().next() {
                file.id.context("folder entry missing id")?
            } else {
                let mut req = File::default();
                req.name = Some(name.to_string());
                req.mime_type = Some(FOLDER_MIME.to_string());
                req.parents = Some(vec![parent_id.to_string()]);

                let (_, created) = hub
                    .files()
                    .create(req)
                    .param("fields", "id")
                    .add_scope(DRIVE_SCOPE)
                    .upload(std::io::empty(), "application/octet-stream".parse::<Mime>()?)
                    .await
                    .context("failed to create folder")?;

                created.id.context("created folder missing id")?
            }
        } else {
            let mut req = File::default();
            req.name = Some(name.to_string());
            req.mime_type = Some(FOLDER_MIME.to_string());
            req.parents = Some(vec![parent_id.to_string()]);

            let (_, created) = hub
                .files()
                .create(req)
                .param("fields", "id")
                .add_scope(DRIVE_SCOPE)
                .upload(std::io::empty(), "application/octet-stream".parse::<Mime>()?)
                .await
                .context("failed to create folder")?;

            created.id.context("created folder missing id")?
        };

        self.path_cache.lock().unwrap().insert(cache_key, id.clone());
        Ok(id)
    }

    async fn resolve_path(&self, virtual_path: &str) -> Result<Option<File>> {
        let sandbox_id = self.ensure_sandbox_id().await?;
        let normalized = Self::normalize_remote_path(virtual_path);
        let segments = Self::split_segments(&normalized);
        if segments.is_empty() {
            return Ok(None);
        }

        let mut parent_id = sandbox_id;

        for segment in &segments[..segments.len() - 1] {
            let cache_key = format!("{parent_id}/{segment}");
            let cached = self.path_cache.lock().unwrap().get(&cache_key).cloned();
            if let Some(id) = cached {
                parent_id = id;
                continue;
            }

            let mut hub = self.hub.lock().await;
            let query = format!(
                "name = '{}' and mimeType = '{}' and trashed = false and '{}' in parents",
                segment.replace('\'', "\\'"),
                FOLDER_MIME,
                parent_id
            );
            let (_, list) = hub
                .files()
                .list()
                .q(&query)
                .spaces("drive")
                .param("fields", "files(id,name,parents)")
                .add_scope(DRIVE_SCOPE)
                .doit()
                .await
                .context("failed to resolve folder")?;

            let Some(file) = list.files.and_then(|mut files| files.pop()) else {
                return Ok(None);
            };

            let id = file.id.context("resolved folder missing id")?;
            self.path_cache.lock().unwrap().insert(cache_key, id.clone());
            parent_id = id;
        }

        let name = segments.last().context("missing last segment")?;
        let mut hub = self.hub.lock().await;
        let query = format!(
            "name = '{}' and trashed = false and '{}' in parents",
            name.replace('\'', "\\'"),
            parent_id
        );
        let (_, list) = hub
            .files()
            .list()
            .q(&query)
            .spaces("drive")
            .param("fields", "files(id,name,parents,mimeType,md5Checksum,modifiedTime,trashed)")
            .add_scope(DRIVE_SCOPE)
            .doit()
            .await
            .context("failed to resolve file")?;

        Ok(list.files.and_then(|mut files| files.pop()))
    }

    async fn ensure_parent_chain(&self, virtual_path: &str) -> Result<String> {
        let sandbox_id = self.ensure_sandbox_id().await?;
        let normalized = Self::normalize_remote_path(virtual_path);
        let segments = Self::split_segments(&normalized);
        if segments.len() <= 1 {
            return Ok(sandbox_id);
        }

        let mut parent_id = sandbox_id;
        for segment in &segments[..segments.len() - 1] {
            parent_id = self.resolve_or_create_folder(&parent_id, segment).await?;
        }
        Ok(parent_id)
    }

    fn file_to_meta(file: &File, virtual_path: &str) -> Result<RemoteFileMeta> {
        let drive_id = file.id.clone().context("file missing id")?;
        let modified_time = file
            .modified_time
            .map(|dt| dt.to_rfc3339())
            .unwrap_or_else(|| "".to_string());
        let md5_hash = file.md5_checksum.clone().unwrap_or_else(|| "".to_string());

        Ok(RemoteFileMeta {
            drive_id,
            virtual_path: virtual_path.to_string(),
            md5_hash,
            modified_time,
        })
    }

    async fn resolve_parents_chain(&self, file_id: &str) -> Result<Vec<File>> {
        let mut hub = self.hub.lock().await;
        let mut chain = Vec::new();
        let mut current_id = file_id.to_string();

        loop {
            let (_, file) = hub
                .files()
                .get(&current_id)
                .param("fields", "id,name,parents,mimeType")
                .add_scope(DRIVE_SCOPE)
                .doit()
                .await
                .context("failed to resolve parent chain")?;

            let parents = file.parents.clone().unwrap_or_default();
            chain.push(file.clone());
            if let Some(parent) = parents.into_iter().next() {
                current_id = parent;
            } else {
                break;
            }
        }

        Ok(chain)
    }

    async fn build_virtual_path_by_chain(&self, file: &File, sandbox_id: &str) -> Result<Option<String>> {
        let file_id = file.id.clone().context("missing file id")?;
        let chain = self.resolve_parents_chain(&file_id).await?;

        let mut names = Vec::new();
        let mut found_sandbox = false;
        for node in chain {
            if let Some(id) = node.id.clone() {
                if id == sandbox_id {
                    found_sandbox = true;
                    break;
                }
            }
            if let Some(name) = node.name.clone() {
                names.push(name);
            }
        }

        if !found_sandbox {
            return Ok(None);
        }

        names.reverse();
        if names.is_empty() {
            Ok(None)
        } else {
            Ok(Some(names.join("/")))
        }
    }

    fn write_body_to_file(body: &Bytes, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }

        let mut file = fs::File::create(path)
            .with_context(|| format!("failed to create {}", path.display()))?;
        file.write_all(body)
            .with_context(|| format!("failed to write {}", path.display()))?;
        Ok(())
    }

    async fn read_body_bytes(body: http_body_util::combinators::BoxBody<Bytes, hyper::Error>) -> Result<Bytes> {
        let collected = body.collect().await?;
        Ok(collected.to_bytes())
    }
}

#[async_trait::async_trait]
impl RemoteStore for GoogleDriveRemoteStore {
    async fn ensure_sandbox(&self) -> Result<()> {
        let _ = self.ensure_sandbox_id().await?;
        Ok(())
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
            bail!("upload_or_update supports files only, got directory {}", local_path.display());
        }

        let normalized = Self::normalize_remote_path(virtual_path);
        let existing = self.resolve_path(&normalized).await?;
        let mime_type: Mime = "application/octet-stream".parse()?;
        let parent_id = self.ensure_parent_chain(&normalized).await?;

        if let Some(file) = existing {
            let file_id = file.id.clone().context("existing file missing id")?;
            let mut req = File::default();
            req.name = file.name.clone();
            let current_parent = file
                .parents
                .clone()
                .and_then(|mut parents| parents.pop());
            if let Some(parent) = current_parent {
                if parent != parent_id {
                    req.parents = Some(vec![parent_id]);
                }
            }

            let mut hub = self.hub.lock().await;
            let (_, updated) = hub
                .files()
                .update(req, &file_id)
                .param("fields", FILE_FIELDS)
                .add_scope(DRIVE_SCOPE)
                .upload(fs::File::open(local_path)?, mime_type)
                .await
                .context("failed to update drive file")?;

            return Self::file_to_meta(&updated, &normalized);
        }

        let mut req = File::default();
        req.name = Some(
            Path::new(&normalized)
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("file")
                .to_string(),
        );
        req.parents = Some(vec![parent_id]);

        let mut hub = self.hub.lock().await;
        let (_, created) = hub
            .files()
            .create(req)
            .param("fields", FILE_FIELDS)
            .add_scope(DRIVE_SCOPE)
            .upload(fs::File::open(local_path)?, mime_type)
            .await
            .context("failed to upload drive file")?;

        Self::file_to_meta(&created, &normalized)
    }

    async fn download_to_local(
        &self,
        virtual_path: &str,
        local_path: &Path,
    ) -> Result<RemoteFileMeta> {
        let normalized = Self::normalize_remote_path(virtual_path);
        let file = self
            .resolve_path(&normalized)
            .await?
            .context("remote file not found")?;
        let file_id = file.id.clone().context("remote file missing id")?;

        let mut hub = self.hub.lock().await;
        let (response, _) = hub
            .files()
            .get(&file_id)
            .param("alt", "media")
            .add_scope(DRIVE_SCOPE)
            .doit()
            .await
            .context("failed to download drive file")?;

        let bytes = Self::read_body_bytes(response.into_body()).await?;

        Self::write_body_to_file(&bytes, local_path)?;
        Self::file_to_meta(&file, &normalized)
    }

    async fn rename(
        &self,
        from_virtual_path: &str,
        to_virtual_path: &str,
    ) -> Result<RemoteFileMeta> {
        let from_normalized = Self::normalize_remote_path(from_virtual_path);
        let to_normalized = Self::normalize_remote_path(to_virtual_path);

        let file = self
            .resolve_path(&from_normalized)
            .await?
            .context("remote file not found")?;
        let file_id = file.id.clone().context("remote file missing id")?;

        let from_parent = file
            .parents
            .clone()
            .and_then(|mut parents| parents.pop())
            .context("remote file missing parent")?;
        let to_parent = self.ensure_parent_chain(&to_normalized).await?;

        let new_name = Path::new(&to_normalized)
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("file")
            .to_string();

        let mut req = File::default();
        req.name = Some(new_name);

        let mut hub = self.hub.lock().await;
        let mut call = hub
            .files()
            .update(req, &file_id)
            .param("fields", FILE_FIELDS)
            .add_scope(DRIVE_SCOPE);

        if from_parent != to_parent {
            call = call.add_parents(&to_parent).remove_parents(&from_parent);
        }

        let (_, updated) = call
            .doit_without_upload()
            .await
            .context("failed to rename drive file")?;

        Self::file_to_meta(&updated, &to_normalized)
    }

    async fn trash(&self, virtual_path: &str) -> Result<()> {
        let normalized = Self::normalize_remote_path(virtual_path);
        let file = self.resolve_path(&normalized).await?;
        let Some(file) = file else {
            return Ok(());
        };

        let file_id = file.id.clone().context("remote file missing id")?;
        let mut req = File::default();
        req.trashed = Some(true);

        let mut hub = self.hub.lock().await;
        hub.files()
            .update(req, &file_id)
            .add_scope(DRIVE_SCOPE)
            .doit_without_upload()
            .await
            .context("failed to trash drive file")?;

        Ok(())
    }

    async fn poll_changes(&self) -> Result<Vec<RemoteChange>> {
        let sandbox_id = self.ensure_sandbox_id().await?;

        let mut page_token_guard = self.page_token.lock().await;
        if page_token_guard.is_none() {
            if let Some(saved) = self.load_page_token().await? {
                *page_token_guard = Some(saved);
            }
        }

        if page_token_guard.is_none() {
            let mut hub = self.hub.lock().await;
            let (_, token_resp) = hub
                .changes()
                .get_start_page_token()
                .add_scope(DRIVE_SCOPE)
                .doit()
                .await
                .context("failed to get start page token")?;
            *page_token_guard = token_resp.start_page_token;
        }

        let mut changes_out = Vec::new();
        let mut next_token = page_token_guard.clone().unwrap();

        loop {
            let mut hub = self.hub.lock().await;
            let (_, change_list) = hub
                .changes()
                .list(&next_token)
                .include_removed(true)
                .spaces("drive")
                .param("fields", CHANGE_FIELDS)
                .add_scope(DRIVE_SCOPE)
                .doit()
                .await
                .context("failed to list drive changes")?;

            if let Some(changes) = change_list.changes {
                for change in changes {
                    if let Some(event) = self.change_to_event(&change, &sandbox_id).await? {
                        changes_out.push(event);
                    }
                }
            }

            if let Some(token) = change_list.next_page_token {
                next_token = token;
            } else {
                if let Some(new_token) = change_list.new_start_page_token {
                    let token_str = new_token;
                    self.save_page_token(&token_str).await?;
                    *page_token_guard = Some(token_str);
                }
                break;
            }
        }

        Ok(changes_out)
    }

    async fn get_metadata(&self, virtual_path: &str) -> Result<Option<RemoteFileMeta>> {
        let normalized = Self::normalize_remote_path(virtual_path);
        let file = self.resolve_path(&normalized).await?;
        let Some(file) = file else {
            return Ok(None);
        };
        let meta = Self::file_to_meta(&file, &normalized)?;
        Ok(Some(meta))
    }
}

impl GoogleDriveRemoteStore {
    async fn change_to_event(
        &self,
        change: &Change,
        sandbox_id: &str,
    ) -> Result<Option<RemoteChange>> {
        if change.removed.unwrap_or(false) {
            if let Some(file_id) = change.file_id.as_ref() {
                if let Some(virtual_path) = self.virtual_path_from_id(file_id, sandbox_id).await? {
                    return Ok(Some(RemoteChange::Delete { virtual_path }));
                }
            }
            return Ok(None);
        }

        let Some(file) = change.file.as_ref() else {
            return Ok(None);
        };

        if file.trashed.unwrap_or(false) {
            if let Some(file_id) = change.file_id.as_ref() {
                if let Some(virtual_path) = self.virtual_path_from_id(file_id, sandbox_id).await? {
                    return Ok(Some(RemoteChange::Delete { virtual_path }));
                }
            }
            return Ok(None);
        }

        if file.mime_type.as_deref() == Some(FOLDER_MIME) {
            return Ok(None);
        }

        let virtual_path = match self.build_virtual_path_by_chain(file, sandbox_id).await? {
            Some(path) => path,
            None => return Ok(None),
        };

        let meta = Self::file_to_meta(file, &virtual_path)?;
        Ok(Some(RemoteChange::Upsert(meta)))
    }

    async fn virtual_path_from_id(&self, file_id: &str, sandbox_id: &str) -> Result<Option<String>> {
        let mut hub = self.hub.lock().await;
        let (_, file) = hub
            .files()
            .get(file_id)
            .param("fields", FILE_FIELDS)
            .add_scope(DRIVE_SCOPE)
            .doit()
            .await
            .context("failed to fetch file metadata for change")?;

        self.build_virtual_path_by_chain(&file, sandbox_id).await
    }
}
