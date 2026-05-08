use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::events::SyncEvent;
use crate::remote::RemoteStore;

pub async fn run_remote_poller(
    remote: Arc<dyn RemoteStore>,
    tx: mpsc::Sender<SyncEvent>,
    poll_every: Duration,
    shutdown: CancellationToken,
) -> Result<()> {
    let mut interval = tokio::time::interval(poll_every);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    info!("remote poller started (every {}s)", poll_every.as_secs());

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("remote poller received shutdown signal");
                break;
            }
            _ = interval.tick() => {
                match remote.poll_changes() {
                    Ok(changes) => {
                        for change in changes {
                            if tx.send(SyncEvent::Remote(change)).await.is_err() {
                                return Ok(());
                            }
                        }
                    }
                    Err(err) => {
                        error!("remote poll failed: {err:#}");
                    }
                }
            }
        }
    }

    Ok(())
}
