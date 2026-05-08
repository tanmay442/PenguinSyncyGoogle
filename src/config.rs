use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::path::{Component, Path, PathBuf};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AppConfig {
    #[serde(default = "default_remote_target_folder")]
    pub remote_target_folder: String,
    #[serde(default = "default_poll_interval_seconds")]
    pub poll_interval_seconds: u64,
    #[serde(default)]
    pub watch: Vec<WatchEntry>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WatchEntry {
    pub local_path: PathBuf,
    pub remote_name: String,
}

impl AppConfig {
    pub fn load_or_create(config_path: &Path) -> Result<Self> {
        if !config_path.exists() {
            if let Some(parent) = config_path.parent() {
                std::fs::create_dir_all(parent).with_context(|| {
                    format!(
                        "failed to create parent directory for config file: {}",
                        parent.display()
                    )
                })?;
            }

            std::fs::write(config_path, default_config_template()).with_context(|| {
                format!(
                    "failed to write default config at {}",
                    config_path.display()
                )
            })?;
        }

        let raw = std::fs::read_to_string(config_path)
            .with_context(|| format!("failed to read {}", config_path.display()))?;

        let mut cfg: AppConfig = toml::from_str(&raw)
            .with_context(|| format!("failed to parse {}", config_path.display()))?;
        cfg.normalize()?;
        Ok(cfg)
    }

    pub fn normalize(&mut self) -> Result<()> {
        self.remote_target_folder = normalize_remote_name(&self.remote_target_folder);
        if self.remote_target_folder.is_empty() {
            self.remote_target_folder = default_remote_target_folder();
        }

        if self.poll_interval_seconds == 0 {
            self.poll_interval_seconds = default_poll_interval_seconds();
        }

        for entry in &mut self.watch {
            let expanded = expand_tilde(&entry.local_path)?;
            entry.local_path = to_absolute(expanded)?;
            entry.remote_name = normalize_remote_name(&entry.remote_name);
            if entry.remote_name.is_empty() {
                bail!(
                    "watch entry with local_path={} has empty remote_name",
                    entry.local_path.display()
                );
            }
        }

        Ok(())
    }

    pub fn local_to_remote(&self, local_path: &Path) -> Option<String> {
        let local_abs = absolutize_runtime(local_path);

        let best = self
            .watch
            .iter()
            .filter(|watch| {
                local_abs == watch.local_path || local_abs.starts_with(&watch.local_path)
            })
            .max_by_key(|watch| watch.local_path.as_os_str().len())?;

        let rel = local_abs.strip_prefix(&best.local_path).ok()?;
        let rel_str = path_to_slash_string(rel);

        if rel_str.is_empty() {
            Some(best.remote_name.clone())
        } else {
            Some(format!("{}/{}", best.remote_name, rel_str))
        }
    }

    pub fn remote_to_local(&self, remote_path: &str) -> Option<PathBuf> {
        let normalized_remote = normalize_remote_name(remote_path);

        let best = self
            .watch
            .iter()
            .filter(|watch| {
                normalized_remote == watch.remote_name
                    || normalized_remote.starts_with(&format!("{}/", watch.remote_name))
            })
            .max_by_key(|watch| watch.remote_name.len())?;

        if normalized_remote == best.remote_name {
            return Some(best.local_path.clone());
        }

        let rel = normalized_remote
            .strip_prefix(&format!("{}/", best.remote_name))
            .unwrap_or_default();

        Some(best.local_path.join(slash_string_to_path(rel)))
    }
}

fn default_remote_target_folder() -> String {
    "guploadsync".to_string()
}

fn default_poll_interval_seconds() -> u64 {
    30
}

fn default_config_template() -> String {
    r#"remote_target_folder = "guploadsync"
poll_interval_seconds = 30

# Add one or more entries to watch and sync.
#
# [[watch]]
# local_path = "/home/username/.bashrc"
# remote_name = "dotfiles/bashrc"
#
# [[watch]]
# local_path = "/home/username/Projects/my_app"
# remote_name = "projects/my_app"
"#
    .to_string()
}

fn normalize_remote_name(raw: &str) -> String {
    raw.trim()
        .trim_matches('/')
        .replace('\\', "/")
        .split('/')
        .filter(|part| !part.is_empty() && *part != ".")
        .collect::<Vec<_>>()
        .join("/")
}

fn slash_string_to_path(raw: &str) -> PathBuf {
    let mut out = PathBuf::new();
    for segment in raw.split('/') {
        if !segment.is_empty() {
            out.push(segment);
        }
    }
    out
}

fn path_to_slash_string(path: &Path) -> String {
    path.components()
        .filter_map(|component| match component {
            Component::Normal(value) => Some(value.to_string_lossy().to_string()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn expand_tilde(path: &Path) -> Result<PathBuf> {
    let raw = path.to_string_lossy();
    if raw == "~" || raw.starts_with("~/") {
        let home =
            dirs::home_dir().context("could not determine home directory for ~ expansion")?;
        if raw == "~" {
            return Ok(home);
        }
        let suffix = raw.trim_start_matches("~/");
        return Ok(home.join(suffix));
    }
    Ok(path.to_path_buf())
}

fn to_absolute(path: PathBuf) -> Result<PathBuf> {
    let abs = if path.is_absolute() {
        path
    } else {
        std::env::current_dir()
            .context("failed to determine current directory")?
            .join(path)
    };

    if abs.exists() {
        Ok(abs.canonicalize().unwrap_or(abs))
    } else {
        Ok(abs)
    }
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
