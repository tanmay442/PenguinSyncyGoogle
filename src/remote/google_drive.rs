use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use google_drive3::api::{Change, File as DriveFile};
use google_drive3::{DriveHub, Error as DriveError, common, hyper, hyper_rustls, hyper_util, yup_oauth2};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File as StdFile};
use std::io::Cursor;
use std::path::{Path, PathBuf};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::events::{RemoteChange, RemoteFileMeta};
use crate::state_db::StateDb;

use super::RemoteStore;

const FOLDER_MIME_TYPE: &str = "application/vnd.google-apps.folder";
const DRIVE_FILE_FIELDS: &str = "id,name,parents,mimeType,md5Checksum,modifiedTime,trashed";
const DRIVE_FILE_LIST_FIELDS: &str = "files(id,name,parents,mimeType,md5Checksum,modifiedTime,trashed)";
const DRIVE_CHANGE_FIELDS: &str = "nextPageToken,newStartPageToken,changes(changeType,fileId,removed,file(id,name,parents,mimeType,md5Checksum,modifiedTime,trashed))";
const PAGE_TOKEN_KEY: &str = "google_drive_page_token";

type HttpConnector = hyper_util::client::legacy::connect::HttpConnector;
type HttpsConnector = hyper_rustls::HttpsConnector<HttpConnector>;
type GoogleDriveHub = DriveHub<HttpsConnector>;

pub struct GoogleDriveRemoteStore {
    hub: GoogleDriveHub,
    sandbox_name: String,
    sandbox_id: RwLock<Option<String>>,
    folder_cache: RwLock<HashMap<String, String>>,
    db_path: PathBuf,
}

impl GoogleDriveRemoteStore {
    pub async fn new(
        sandbox_name: String,
        credentials_file: PathBuf,
        token_cache_file: PathBuf,
        db_path: PathBuf,
    ) -> Result<Self> {
        if !credentials_file.exists() {
            bail!(
                "google OAuth client secret file not found at {}",
                credentials_file.display()
            );
        }

        if let Some(parent) = token_cache_file.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }

        let secret = yup_oauth2::read_application_secret(&credentials_file)
            .await
            .with_context(|| {
                format!(
                    "failed to read OAuth client secret from {}",
                    credentials_file.display()
                )
            })?;

        let auth = yup_oauth2::InstalledFlowAuthenticator::builder(
            secret,
            yup_oauth2::InstalledFlowReturnMethod::HTTPRedirect,
        )
        .persist_tokens_to_disk(&token_cache_file)
        .build()
        .await
        .with_context(|| {
            format!(
                "failed to initialize OAuth authenticator using {}",
                token_cache_file.display()
            )
        })?;

        let connector = hyper_rustls::HttpsConnectorBuilder::new()
            .with_native_roots()
            .context("failed to load native TLS root certificates")?
            .https_or_http()
            .enable_http2()
            .build();

        let client = hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
            .build(connector);

        let hub = DriveHub::new(client, auth);

        Ok(Self {
            hub,
            sandbox_name: normalize_sandbox_name(&sandbox_name),
            sandbox_id: RwLock::new(None),
            folder_cache: RwLock::new(HashMap::new()),
            db_path,
        })
    }

    async fn ensure_sandbox_id(&self) -> Result<String> {
        if let Some(id) = self.sandbox_id.read().await.clone() {
            return Ok(id);
        }

        let folder = match self
            .find_child("root", &self.sandbox_name, Some(FOLDER_MIME_TYPE), None)
            .await?
        {
            Some(folder) => folder,
            None => {
                info!("creating Drive sandbox folder '{}'", self.sandbox_name);
                self.create_folder("root", &self.sandbox_name).await?
            }
        };

        let folder_id = folder
            .id
            .context("Drive returned sandbox folder without id")?;

        *self.sandbox_id.write().await = Some(folder_id.clone());

        Ok(folder_id)
    }

    async fn create_folder(&self, parent_id: &str, folder_name: &str) -> Result<DriveFile> {
        let request = DriveFile {
            name: Some(folder_name.to_string()),
            mime_type: Some(FOLDER_MIME_TYPE.to_string()),
            parents: Some(vec![parent_id.to_string()]),
            ..Default::default()
        };

        let (_, folder) = self
            .hub
            .files()
            .create(request)
            .supports_all_drives(true)
            .param("fields", DRIVE_FILE_FIELDS)
            .upload(Cursor::new(Vec::new()), mime::APPLICATION_OCTET_STREAM)
            .await
            .with_context(|| {
                format!(
                    "failed to create Drive folder '{}' under parent {}",
                    folder_name, parent_id
                )
            })?;

        Ok(folder)
    }

    async fn find_child(
        &self,
        parent_id: &str,
        name: &str,
        exact_mime_type: Option<&str>,
        exclude_mime_type: Option<&str>,
    ) -> Result<Option<DriveFile>> {
        let escaped_parent = escape_drive_query_literal(parent_id);
        let escaped_name = escape_drive_query_literal(name);

        let mut query = format!(
            "name = '{}' and '{}' in parents and trashed = false",
            escaped_name, escaped_parent
        );

        if let Some(mime_type) = exact_mime_type {
            query.push_str(&format!(
                " and mimeType = '{}'",
                escape_drive_query_literal(mime_type)
            ));
        }

        if let Some(mime_type) = exclude_mime_type {
            query.push_str(&format!(
                " and mimeType != '{}'",
                escape_drive_query_literal(mime_type)
            ));
        }

        let (_, list) = self
            .hub
            .files()
            .list()
            .q(&query)
            .spaces("drive")
            .supports_all_drives(true)
            .include_items_from_all_drives(true)
            .page_size(10)
            .param("fields", DRIVE_FILE_LIST_FIELDS)
            .doit()
            .await
            .with_context(|| {
                format!(
                    "failed to list Drive children for parent {} and name '{}'",
                    parent_id, name
                )
            })?;

        Ok(list.files.and_then(|mut files| files.drain(..).next()))
    }

    async fn ensure_folder_chain(&self, folders: &[String]) -> Result<String> {
        let mut parent_id = self.ensure_sandbox_id().await?;
        let mut running_path = String::new();

        for segment in folders {
            if !running_path.is_empty() {
                running_path.push('/');
            }
            running_path.push_str(segment);

            if let Some(cached_id) = self.folder_cache.read().await.get(&running_path).cloned() {
                parent_id = cached_id;
                continue;
            }

            let folder = match self
                .find_child(&parent_id, segment, Some(FOLDER_MIME_TYPE), None)
                .await?
            {
                Some(existing) => existing,
                None => self.create_folder(&parent_id, segment).await?,
            };

            let folder_id = folder
                .id
                .context("Drive folder lookup returned object without id")?;

            self.folder_cache
                .write()
                .await
                .insert(running_path.clone(), folder_id.clone());

            parent_id = folder_id;
        }

        Ok(parent_id)
    }

    async fn resolve_file_by_virtual_path(
        &self,
        virtual_path: &str,
        expect_file: bool,
    ) -> Result<Option<DriveFile>> {
        let segments = split_virtual_path(virtual_path)?;
        let mut parent_id = self.ensure_sandbox_id().await?;
        let mut running_folder_path = String::new();
        let mut current: Option<DriveFile> = None;

        for (index, segment) in segments.iter().enumerate() {
            let is_last = index + 1 == segments.len();

            let (exact_mime, exclude_mime) = if is_last {
                if expect_file {
                    (None, Some(FOLDER_MIME_TYPE))
                } else {
                    (None, None)
                }
            } else {
                if !running_folder_path.is_empty() {
                    running_folder_path.push('/');
                }
                running_folder_path.push_str(segment);
                (Some(FOLDER_MIME_TYPE), None)
            };

            let Some(found) = self
                .find_child(&parent_id, segment, exact_mime, exclude_mime)
                .await?
            else {
                return Ok(None);
            };

            let found_id = found
                .id
                .clone()
                .context("Drive path resolver found item without id")?;

            if !is_last {
                self.folder_cache
                    .write()
                    .await
                    .insert(running_folder_path.clone(), found_id.clone());
            }

            parent_id = found_id;
            current = Some(found);
        }

        Ok(current)
    }

    async fn get_file_by_id(&self, file_id: &str) -> Result<Option<DriveFile>> {
        let result = self
            .hub
            .files()
            .get(file_id)
            .supports_all_drives(true)
            .param("fields", DRIVE_FILE_FIELDS)
            .doit()
            .await;

        match result {
            Ok((_, file)) => Ok(Some(file)),
            Err(err) if is_not_found(&err) => Ok(None),
            Err(err) => Err(anyhow!("failed to fetch Drive file metadata for {file_id}: {err}")),
        }
    }

    async fn fetch_start_page_token(&self) -> Result<String> {
        let (_, response) = self
            .hub
            .changes()
            .get_start_page_token()
            .supports_all_drives(true)
            .doit()
            .await
            .context("failed to fetch Drive start page token")?;

        response
            .start_page_token
            .context("Drive did not return a startPageToken")
    }

    fn load_page_token(&self) -> Result<Option<String>> {
        let db = StateDb::open(&self.db_path)?;
        db.get_meta(PAGE_TOKEN_KEY)
    }

    fn save_page_token(&self, token: &str) -> Result<()> {
        let db = StateDb::open(&self.db_path)?;
        db.set_meta(PAGE_TOKEN_KEY, token)
    }

    fn lookup_virtual_path_by_drive_id(&self, drive_id: &str) -> Result<Option<String>> {
        let db = StateDb::open(&self.db_path)?;
        Ok(db
            .get_by_drive_id(drive_id)?
            .map(|record| record.virtual_remote_path))
    }

    fn to_remote_file_meta(&self, file: &DriveFile, virtual_path: String) -> Result<RemoteFileMeta> {
        let drive_id = file
            .id
            .clone()
            .context("Drive metadata missing file id")?;

        let modified_time = file
            .modified_time
            .as_ref()
            .map(chrono::DateTime::to_rfc3339)
            .unwrap_or_else(|| chrono::Utc::now().to_rfc3339());

        Ok(RemoteFileMeta {
            drive_id,
            virtual_path,
            md5_hash: file.md5_checksum.clone().unwrap_or_default(),
            modified_time,
        })
    }

    async fn virtual_path_from_file(&self, file: &DriveFile) -> Result<Option<String>> {
        let file_name = match file.name.clone() {
            Some(name) if !name.is_empty() => name,
            _ => return Ok(None),
        };

        let sandbox_id = self.ensure_sandbox_id().await?;
        let mut segments = vec![file_name];
        let mut parent_id = file
            .parents
            .as_ref()
            .and_then(|parents| parents.first().cloned());

        let mut visited = HashSet::new();
        let mut reached_sandbox = false;

        while let Some(current_parent_id) = parent_id {
            if current_parent_id == sandbox_id {
                reached_sandbox = true;
                break;
            }

            if !visited.insert(current_parent_id.clone()) {
                return Ok(None);
            }

            let Some(parent) = self.get_file_by_id(&current_parent_id).await? else {
                return Ok(None);
            };

            if parent.trashed.unwrap_or(false) {
                return Ok(None);
            }

            let Some(parent_name) = parent.name else {
                return Ok(None);
            };

            segments.push(parent_name);
            parent_id = parent.parents.and_then(|mut p| p.drain(..).next());
        }

        if !reached_sandbox {
            return Ok(None);
        }

        segments.reverse();
        let normalized = normalize_virtual_path(&segments.join("/"));

        if normalized.is_empty() {
            return Ok(None);
        }

        Ok(Some(normalized))
    }

    async fn translate_change(&self, change: Change) -> Result<Option<RemoteChange>> {
        if matches!(change.change_type.as_deref(), Some("drive")) {
            return Ok(None);
        }

        let file_id = change
            .file_id
            .clone()
            .or_else(|| change.file.as_ref().and_then(|f| f.id.clone()));

        let mut mapped_path: Option<String> = None;
        let mut removed_or_trashed = change.removed.unwrap_or(false);

        if let Some(file) = change.file.as_ref() {
            if file.mime_type.as_deref() == Some(FOLDER_MIME_TYPE) {
                return Ok(None);
            }

            removed_or_trashed |= file.trashed.unwrap_or(false);
            mapped_path = self.virtual_path_from_file(file).await?;
        }

        if removed_or_trashed {
            if let Some(path) = mapped_path {
                return Ok(Some(RemoteChange::Delete { virtual_path: path }));
            }

            if let Some(file_id) = file_id
                && let Some(path) = self.lookup_virtual_path_by_drive_id(&file_id)?
            {
                return Ok(Some(RemoteChange::Delete { virtual_path: path }));
            }

            return Ok(None);
        }

        if let Some(file) = change.file.as_ref()
            && let Some(path) = mapped_path
        {
            let meta = self.to_remote_file_meta(file, path)?;
            return Ok(Some(RemoteChange::Upsert(meta)));
        }

        if let Some(file_id) = file_id {
            if let Some(file) = self.get_file_by_id(&file_id).await? {
                if file.mime_type.as_deref() == Some(FOLDER_MIME_TYPE) {
                    return Ok(None);
                }

                if file.trashed.unwrap_or(false) {
                    if let Some(path) = self.lookup_virtual_path_by_drive_id(&file_id)? {
                        return Ok(Some(RemoteChange::Delete { virtual_path: path }));
                    }
                    return Ok(None);
                }

                if let Some(path) = self.virtual_path_from_file(&file).await? {
                    let meta = self.to_remote_file_meta(&file, path)?;
                    return Ok(Some(RemoteChange::Upsert(meta)));
                }

                if let Some(path) = self.lookup_virtual_path_by_drive_id(&file_id)? {
                    return Ok(Some(RemoteChange::Delete { virtual_path: path }));
                }
            } else if let Some(path) = self.lookup_virtual_path_by_drive_id(&file_id)? {
                return Ok(Some(RemoteChange::Delete { virtual_path: path }));
            }
        }

        Ok(None)
    }
}

#[async_trait]
impl RemoteStore for GoogleDriveRemoteStore {
    async fn ensure_sandbox(&self) -> Result<()> {
        let id = self.ensure_sandbox_id().await?;
        debug!("using Drive sandbox '{}' (id={id})", self.sandbox_name);
        Ok(())
    }

    async fn upload_or_update(&self, virtual_path: &str, local_path: &Path) -> Result<RemoteFileMeta> {
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
        let (folder_segments, file_name) = split_parent_and_file_name(&normalized)?;
        let parent_id = self.ensure_folder_chain(&folder_segments).await?;

        let existing = self
            .find_child(&parent_id, &file_name, None, Some(FOLDER_MIME_TYPE))
            .await?;

        let uploaded_file = if let Some(existing) = existing {
            let file_id = existing
                .id
                .context("Drive existing file missing id during update")?;

            let stream = StdFile::open(local_path)
                .with_context(|| format!("failed to open {} for upload", local_path.display()))?;

            let (_, updated) = self
                .hub
                .files()
                .update(DriveFile::default(), &file_id)
                .supports_all_drives(true)
                .param("fields", DRIVE_FILE_FIELDS)
                .upload_resumable(stream, mime::APPLICATION_OCTET_STREAM)
                .await
                .with_context(|| {
                    format!(
                        "failed Drive update upload for {} ({})",
                        normalized,
                        local_path.display()
                    )
                })?;

            updated
        } else {
            let request = DriveFile {
                name: Some(file_name),
                parents: Some(vec![parent_id]),
                ..Default::default()
            };

            let stream = StdFile::open(local_path)
                .with_context(|| format!("failed to open {} for upload", local_path.display()))?;

            let (_, created) = self
                .hub
                .files()
                .create(request)
                .supports_all_drives(true)
                .param("fields", DRIVE_FILE_FIELDS)
                .upload_resumable(stream, mime::APPLICATION_OCTET_STREAM)
                .await
                .with_context(|| {
                    format!(
                        "failed Drive create upload for {} ({})",
                        normalized,
                        local_path.display()
                    )
                })?;

            created
        };

        self.to_remote_file_meta(&uploaded_file, normalized)
    }

    async fn download_to_local(&self, virtual_path: &str, local_path: &Path) -> Result<RemoteFileMeta> {
        let normalized = normalize_virtual_path(virtual_path);

        let Some(file) = self.resolve_file_by_virtual_path(&normalized, true).await? else {
            bail!("remote file does not exist: {normalized}");
        };

        let file_id = file
            .id
            .clone()
            .context("Drive download target missing file id")?;

        let (response, _) = self
            .hub
            .files()
            .get(&file_id)
            .supports_all_drives(true)
            .param("alt", "media")
            .doit()
            .await
            .with_context(|| format!("failed Drive download for {normalized}"))?;

        let bytes = common::to_bytes(response.into_body())
            .await
            .ok_or_else(|| anyhow!("Drive download returned empty body for {normalized}"))?;

        if let Some(parent) = local_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }

        fs::write(local_path, bytes.as_ref())
            .with_context(|| format!("failed to write {}", local_path.display()))?;

        let final_meta = self.get_file_by_id(&file_id).await?.unwrap_or(file);
        self.to_remote_file_meta(&final_meta, normalized)
    }

    async fn rename(&self, from_virtual_path: &str, to_virtual_path: &str) -> Result<RemoteFileMeta> {
        let from_normalized = normalize_virtual_path(from_virtual_path);
        let to_normalized = normalize_virtual_path(to_virtual_path);

        if from_normalized == to_normalized {
            return self
                .get_metadata(&to_normalized)
                .await?
                .context("rename source and destination are equal but metadata is missing");
        }

        let Some(source) = self.resolve_file_by_virtual_path(&from_normalized, true).await? else {
            bail!("remote source does not exist: {from_normalized}");
        };

        let source_id = source
            .id
            .clone()
            .context("Drive source missing id during rename")?;

        let (target_parent_segments, target_name) = split_parent_and_file_name(&to_normalized)?;
        let target_parent_id = self.ensure_folder_chain(&target_parent_segments).await?;

        if let Some(existing_target) = self
            .find_child(&target_parent_id, &target_name, None, Some(FOLDER_MIME_TYPE))
            .await?
        {
            let existing_target_id = existing_target
                .id
                .context("Drive target file missing id")?;

            if existing_target_id != source_id {
                warn!(
                    "target '{}' already exists in Drive; moving existing copy to trash first",
                    to_normalized
                );

                let patch = DriveFile {
                    trashed: Some(true),
                    ..Default::default()
                };

                self.hub
                    .files()
                    .update(patch, &existing_target_id)
                    .supports_all_drives(true)
                    .doit_without_upload()
                    .await
                    .with_context(|| {
                        format!(
                            "failed to trash existing target {} before rename",
                            to_normalized
                        )
                    })?;
            }
        }

        let old_parent = source
            .parents
            .as_ref()
            .and_then(|parents| parents.first())
            .cloned();

        let patch = DriveFile {
            name: Some(target_name),
            ..Default::default()
        };

        let mut call = self
            .hub
            .files()
            .update(patch, &source_id)
            .supports_all_drives(true)
            .param("fields", DRIVE_FILE_FIELDS);

        if old_parent.as_deref() != Some(target_parent_id.as_str()) {
            if let Some(old_parent_id) = old_parent.as_deref() {
                call = call.remove_parents(old_parent_id);
            }
            call = call.add_parents(&target_parent_id);
        }

        let (_, updated) = call
            .doit_without_upload()
            .await
            .with_context(|| {
                format!(
                    "failed Drive rename {} -> {}",
                    from_normalized, to_normalized
                )
            })?;

        self.to_remote_file_meta(&updated, to_normalized)
    }

    async fn trash(&self, virtual_path: &str) -> Result<()> {
        let normalized = normalize_virtual_path(virtual_path);

        let Some(file) = self.resolve_file_by_virtual_path(&normalized, true).await? else {
            return Ok(());
        };

        let file_id = file
            .id
            .clone()
            .context("Drive trash target missing id")?;

        let patch = DriveFile {
            trashed: Some(true),
            ..Default::default()
        };

        self.hub
            .files()
            .update(patch, &file_id)
            .supports_all_drives(true)
            .doit_without_upload()
            .await
            .with_context(|| format!("failed to move Drive file to trash: {normalized}"))?;

        Ok(())
    }

    async fn poll_changes(&self) -> Result<Vec<RemoteChange>> {
        let mut page_token = match self.load_page_token()? {
            Some(token) => token,
            None => {
                let token = self.fetch_start_page_token().await?;
                self.save_page_token(&token)?;
                return Ok(Vec::new());
            }
        };

        let mut events = Vec::new();

        loop {
            let result = self
                .hub
                .changes()
                .list(&page_token)
                .include_removed(true)
                .include_corpus_removals(true)
                .restrict_to_my_drive(true)
                .supports_all_drives(true)
                .include_items_from_all_drives(true)
                .spaces("drive")
                .page_size(1000)
                .param("fields", DRIVE_CHANGE_FIELDS)
                .doit()
                .await;

            let (_, response) = match result {
                Ok(ok) => ok,
                Err(err) if is_gone(&err) => {
                    warn!("Drive changes page token expired; resetting token and continuing");
                    let token = self.fetch_start_page_token().await?;
                    self.save_page_token(&token)?;
                    return Ok(Vec::new());
                }
                Err(err) => {
                    return Err(anyhow!("Drive changes.list failed: {err}"));
                }
            };

            if let Some(changes) = response.changes {
                for change in changes {
                    if let Some(event) = self.translate_change(change).await? {
                        events.push(event);
                    }
                }
            }

            if let Some(next_page_token) = response.next_page_token {
                page_token = next_page_token;
                continue;
            }

            if let Some(new_start_page_token) = response.new_start_page_token {
                self.save_page_token(&new_start_page_token)?;
            } else {
                self.save_page_token(&page_token)?;
            }

            break;
        }

        Ok(events)
    }

    async fn get_metadata(&self, virtual_path: &str) -> Result<Option<RemoteFileMeta>> {
        let normalized = normalize_virtual_path(virtual_path);

        let Some(file) = self.resolve_file_by_virtual_path(&normalized, true).await? else {
            return Ok(None);
        };

        Ok(Some(self.to_remote_file_meta(&file, normalized)?))
    }
}

fn normalize_sandbox_name(raw: &str) -> String {
    let normalized = raw.trim().trim_matches('/').replace(['/', '\\'], "_");
    if normalized.is_empty() {
        "guploadsync".to_string()
    } else {
        normalized
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

fn split_virtual_path(raw: &str) -> Result<Vec<String>> {
    let normalized = normalize_virtual_path(raw);
    if normalized.is_empty() {
        bail!("remote path cannot be empty");
    }

    let mut parts = Vec::new();
    for segment in normalized.split('/') {
        if segment == ".." {
            bail!("invalid remote path segment '..' in {raw}");
        }
        parts.push(segment.to_string());
    }

    if parts.is_empty() {
        bail!("remote path cannot be empty");
    }

    Ok(parts)
}

fn split_parent_and_file_name(raw: &str) -> Result<(Vec<String>, String)> {
    let mut parts = split_virtual_path(raw)?;
    let file_name = parts
        .pop()
        .context("remote path does not contain a file name")?;
    Ok((parts, file_name))
}

fn escape_drive_query_literal(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "\\'")
}

fn error_status_code(err: &DriveError) -> Option<u16> {
    match err {
        DriveError::Failure(response) => Some(response.status().as_u16()),
        DriveError::BadRequest(json) => json
            .get("error")
            .and_then(|error| error.get("code"))
            .and_then(|code| code.as_u64())
            .map(|code| code as u16),
        _ => None,
    }
}

fn is_not_found(err: &DriveError) -> bool {
    error_status_code(err) == Some(hyper::StatusCode::NOT_FOUND.as_u16())
}

fn is_gone(err: &DriveError) -> bool {
    error_status_code(err) == Some(hyper::StatusCode::GONE.as_u16())
}
