use anyhow::Result;
use notify::event::ModifyKind;
use notify::{Event, EventKind, RecursiveMode, Watcher};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::config::AppConfig;
use crate::events::{LocalFileEvent, LocalFileEventKind, SyncEvent};

#[derive(Debug, Clone, Copy)]
enum DebouncedKind {
    Upsert,
    Delete,
}

#[derive(Debug, Clone, Copy)]
struct DebouncedEvent {
    kind: DebouncedKind,
    ready_at: Instant,
}

pub async fn run_local_watcher(
    config: AppConfig,
    tx: mpsc::Sender<SyncEvent>,
    shutdown: CancellationToken,
) -> Result<()> {
    let (raw_tx, mut raw_rx) = mpsc::unbounded_channel::<notify::Result<Event>>();

    let mut watcher = notify::recommended_watcher(move |result| {
        let _ = raw_tx.send(result);
    })?;

    let mut watched_paths = 0_usize;

    for watch in &config.watch {
        let path = &watch.local_path;
        if !path.exists() {
            warn!(
                "watch path does not exist yet, skipping: {}",
                path.display()
            );
            continue;
        }

        let mode = if path.is_dir() {
            RecursiveMode::Recursive
        } else {
            RecursiveMode::NonRecursive
        };

        match watcher.watch(path, mode) {
            Ok(()) => {
                watched_paths += 1;
                info!("watching {}", path.display());
            }
            Err(err) => warn!("failed to watch {}: {err}", path.display()),
        }
    }

    info!("local watcher initialized with {watched_paths} active watch roots");

    let mut pending = HashMap::<PathBuf, DebouncedEvent>::new();
    let debounce_for = Duration::from_secs(2);
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("local watcher received shutdown signal");
                break;
            }
            _ = tick.tick() => {
                if flush_pending(&mut pending, &tx).await.is_err() {
                    break;
                }
            }
            maybe = raw_rx.recv() => {
                let Some(result) = maybe else {
                    warn!("local watcher source channel closed");
                    break;
                };

                match result {
                    Ok(event) => {
                        queue_event(event, &mut pending, debounce_for);
                    }
                    Err(err) => {
                        error!("file watcher error: {err}");
                    }
                }
            }
        }
    }

    Ok(())
}

fn queue_event(
    event: Event,
    pending: &mut HashMap<PathBuf, DebouncedEvent>,
    debounce_for: Duration,
) {
    let now = Instant::now();
    let ready_at = now + debounce_for;

    let mut queue_path = |path: &Path, kind: DebouncedKind| {
        let absolute = absolutize_runtime(path);

        if matches!(kind, DebouncedKind::Upsert) && absolute.is_dir() {
            return;
        }

        pending.insert(absolute, DebouncedEvent { kind, ready_at });
    };

    match event.kind {
        EventKind::Create(_) => {
            for path in &event.paths {
                queue_path(path, DebouncedKind::Upsert);
            }
        }
        EventKind::Modify(ModifyKind::Name(_)) => {
            if event.paths.len() == 2 {
                queue_path(&event.paths[0], DebouncedKind::Delete);
                queue_path(&event.paths[1], DebouncedKind::Upsert);
            } else {
                for path in &event.paths {
                    queue_path(path, DebouncedKind::Upsert);
                }
            }
        }
        EventKind::Modify(
            ModifyKind::Any | ModifyKind::Data(_) | ModifyKind::Metadata(_) | ModifyKind::Other,
        ) => {
            for path in &event.paths {
                queue_path(path, DebouncedKind::Upsert);
            }
        }
        EventKind::Remove(_) => {
            for path in &event.paths {
                queue_path(path, DebouncedKind::Delete);
            }
        }
        _ => {
            debug!("ignored local event kind: {:?}", event.kind);
        }
    }
}

async fn flush_pending(
    pending: &mut HashMap<PathBuf, DebouncedEvent>,
    tx: &mpsc::Sender<SyncEvent>,
) -> Result<()> {
    let now = Instant::now();

    let ready = pending
        .iter()
        .filter_map(|(path, queued)| {
            (queued.ready_at <= now).then_some((path.clone(), queued.kind))
        })
        .collect::<Vec<_>>();

    for (path, kind) in ready {
        pending.remove(&path);

        let kind = match kind {
            DebouncedKind::Upsert => LocalFileEventKind::Upsert,
            DebouncedKind::Delete => LocalFileEventKind::Delete,
        };

        let event = SyncEvent::Local(LocalFileEvent { kind, path });
        if tx.send(event).await.is_err() {
            return Err(anyhow::anyhow!("sync event channel closed"));
        }
    }

    Ok(())
}

fn absolutize_runtime(path: &Path) -> PathBuf {
    if path.is_absolute() {
        if path.exists() {
            path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
        } else {
            path.to_path_buf()
        }
    } else {
        std::env::current_dir()
            .map(|cwd| cwd.join(path))
            .unwrap_or_else(|_| path.to_path_buf())
    }
}
