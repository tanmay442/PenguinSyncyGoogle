# Project Metadata

- **Name:** PenguinSyncyGoogle
- **Binary:** `guploadsync`
- **Language:** Rust (Edition 2024)
- **Primary OS:** Linux
- **Project Type:** Background sync daemon
- **Runtime Model:** Tokio async runtime + event bus
- **Local DB:** SQLite (`~/.config/guploadsync/state.db`)
- **Default Config File:** `~/.config/guploadsync/config.toml`
- **Default Remote Sandbox (current backend):** `~/.local/share/guploadsync/remote_sandbox/guploadsync`
- **Soft Delete Behavior:**
  - Local deletes -> remote trash area
  - Remote deletes -> Linux desktop trash
- **Current Remote Backend:** Filesystem-backed Drive emulator (`FsRemoteStore`)
- **Status:** Project scaffold implemented and compiling (`cargo build` succeeds)
