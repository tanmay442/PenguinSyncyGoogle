mod app_paths;
mod config;
mod events;
mod hash_utils;
mod local_watcher;
mod meta;
mod remote;
mod remote_poller;
mod state_db;
mod sync_engine;

use anyhow::Result;
use clap::Parser;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use tracing_subscriber::{EnvFilter, fmt};

use crate::app_paths::AppPaths;
use crate::config::AppConfig;
use crate::events::SyncEvent;
use crate::meta::{OPERATING_SYSTEM, PROGRAMMING_LANGUAGE, PROJECT_NAME};
use crate::remote::{GoogleDriveRemoteStore, RemoteStore};
use crate::state_db::StateDb;
use crate::sync_engine::SyncEngine;

#[derive(Debug, Parser)]
#[command(name = "guploadsync")]
#[command(about = "Selective, sandboxed sync daemon")]
struct Cli {
    #[arg(long, help = "Run an immediate full sync after startup")]
    sync_now: bool,

    #[arg(long, help = "Run one full sync and exit")]
    once: bool,

    #[arg(long, help = "Override log filter, e.g. info,debug")]
    log: Option<String>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(cli.log.as_deref());

    info!("starting {PROJECT_NAME} ({PROGRAMMING_LANGUAGE}/{OPERATING_SYSTEM})");

    let paths = AppPaths::discover()?;
    paths.ensure_directories()?;

    let config = AppConfig::load_or_create(&paths.config_file)?;

    if !paths.credentials_file.exists() {
        warn!(
            "google oauth credentials not found yet at {}",
            paths.credentials_file.display()
        );
    }

    info!("token cache path: {}", paths.token_cache_file.display());

    if config.watch.is_empty() {
        warn!(
            "no watch entries configured. edit {} to start syncing paths",
            paths.config_file.display()
        );
    }

    let db = StateDb::open(&paths.db_file)?;

    let remote_backend: Arc<dyn RemoteStore> = Arc::new(
        GoogleDriveRemoteStore::new(
            config.remote_target_folder.clone(),
            paths.credentials_file.clone(),
            paths.token_cache_file.clone(),
            paths.db_file.clone(),
        )
        .await?,
    );

    remote_backend.ensure_sandbox().await?;

    let (tx, rx) = mpsc::channel::<SyncEvent>(1024);
    let shutdown = CancellationToken::new();

    let local_task = {
        let config = config.clone();
        let tx = tx.clone();
        let shutdown = shutdown.child_token();
        tokio::spawn(async move {
            if let Err(err) = local_watcher::run_local_watcher(config, tx, shutdown).await {
                error!("local watcher exited with error: {err:#}");
            }
        })
    };

    let remote_task = {
        let tx = tx.clone();
        let remote = remote_backend.clone();
        let shutdown = shutdown.child_token();
        let poll_every = Duration::from_secs(config.poll_interval_seconds);

        tokio::spawn(async move {
            if let Err(err) =
                remote_poller::run_remote_poller(remote, tx, poll_every, shutdown).await
            {
                error!("remote poller exited with error: {err:#}");
            }
        })
    };

    {
        let tx = tx.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                info!("received Ctrl+C, shutting down");
                shutdown.cancel();
                let _ = tx.send(SyncEvent::Shutdown).await;
            }
        });
    }

    if cli.sync_now || cli.once {
        tx.send(SyncEvent::SyncNow).await?;
    }

    if cli.once {
        tx.send(SyncEvent::Shutdown).await?;
    }

    let mut engine = SyncEngine::new(config, db, remote_backend, rx);
    let engine_result = engine.run().await;

    shutdown.cancel();

    local_task.abort();
    remote_task.abort();

    let _ = local_task.await;
    let _ = remote_task.await;

    engine_result
}

fn init_tracing(log_override: Option<&str>) {
    let filter = if let Some(log) = log_override {
        EnvFilter::new(log)
    } else {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
    };

    fmt().with_env_filter(filter).with_target(false).init();
}
