use anyhow::{Context, Result};
use std::path::PathBuf;

use crate::meta::APP_FOLDER_NAME;

#[derive(Debug, Clone)]
pub struct AppPaths {
    pub config_dir: PathBuf,
    pub data_dir: PathBuf,
    pub config_file: PathBuf,
    pub db_file: PathBuf,
    pub credentials_file: PathBuf,
    pub token_cache_file: PathBuf,
}

impl AppPaths {
    pub fn discover() -> Result<Self> {
        let home = dirs::home_dir().context("could not determine home directory")?;
        let cwd = std::env::current_dir().context("could not determine current directory")?;

        let config_root = dirs::config_dir().unwrap_or_else(|| home.join(".config"));
        let data_root = dirs::data_local_dir().unwrap_or_else(|| home.join(".local/share"));

        let config_dir = config_root.join(APP_FOLDER_NAME);
        let data_dir = data_root.join(APP_FOLDER_NAME);

        Ok(Self {
            config_file: config_dir.join("config.toml"),
            db_file: config_dir.join("state.db"),
            credentials_file: cwd.join("clientsecret.json"),
            token_cache_file: config_dir.join("token_cache.json"),
            config_dir,
            data_dir,
        })
    }

    pub fn ensure_directories(&self) -> Result<()> {
        std::fs::create_dir_all(&self.config_dir)
            .with_context(|| format!("failed to create {}", self.config_dir.display()))?;
        std::fs::create_dir_all(&self.data_dir)
            .with_context(|| format!("failed to create {}", self.data_dir.display()))?;
        Ok(())
    }

    pub fn remote_sandbox_dir(&self, target_folder: &str) -> PathBuf {
        self.data_dir.join("sandbox").join(target_folder)
    }

    pub fn remote_trash_dir(&self) -> PathBuf {
        self.data_dir.join("remote_trash")
    }
}
