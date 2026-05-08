# PenguinSyncyGoogle (`guploadsync`)

Rust implementation of the architecture and userflow described in:
- `architecture.md`
- `userflow.md`
- `metadata.md`

## What is implemented

### Core architecture
- **Tokio-based daemon** with async tasks and event bus (`tokio::sync::mpsc`)
- **Local watcher** (`notify`) with **2s debounce**
- **Remote poller** (30s configurable) emitting remote change events
- **Sync engine** with a local **SQLite state database** (`rusqlite`)
- **Soft delete safety**:
  - local delete => remote moved to remote trash
  - remote delete => local moved to Linux desktop trash (`trash` crate)

### State engine & loop prevention
- SQLite schema includes:
  - `local_path`
  - `drive_id`
  - `virtual_remote_path`
  - `md5_hash`
  - `local_timestamp`
  - `remote_timestamp`
- MD5 + timestamps are used to:
  - avoid redundant uploads
  - prevent infinite echo loops
  - reconcile remote changes safely

### Edge-case handling from userflow
- **Rapid save spam** handled by debounce queue
- **Rename detection** via pending-delete + hash matching (5s window)
- **Offline conflict handling**:
  - detect local+remote divergence from DB baseline
  - store remote as `(... conflicted copy ...)`
  - keep local version as active version
- **Delete safety** on both sides

## Important implementation note

This build uses a **filesystem remote backend** (`FsRemoteStore`) to model Drive behavior locally.
It syncs into:

- `~/.local/share/guploadsync/remote_sandbox/<remote_target_folder>`

This keeps the architecture and state engine behavior working end-to-end without requiring immediate OAuth setup.
A Google Drive backend can be added by implementing the `RemoteStore` trait in `src/remote/mod.rs`.

## Configuration

Config path:

- `~/.config/guploadsync/config.toml`

Generated template:

```toml
remote_target_folder = "guploadsync"
poll_interval_seconds = 30

# [[watch]]
# local_path = "/home/username/.bashrc"
# remote_name = "dotfiles/bashrc"
```

## Build & run

```bash
cargo build
cargo run
```

One-shot sync and exit:

```bash
cargo run -- --once
```

Manual full sync on startup:

```bash
cargo run -- --sync-now
```
