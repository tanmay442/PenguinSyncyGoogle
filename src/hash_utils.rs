use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use md5::Context as Md5Context;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;
use std::time::UNIX_EPOCH;

pub fn compute_md5(path: &Path) -> Result<String> {
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut reader = BufReader::new(file);

    let mut context = Md5Context::new();
    let mut buffer = [0_u8; 8192];

    loop {
        let bytes_read = reader
            .read(&mut buffer)
            .with_context(|| format!("failed to read {}", path.display()))?;

        if bytes_read == 0 {
            break;
        }

        context.consume(&buffer[..bytes_read]);
    }

    Ok(format!("{:x}", context.compute()))
}

pub fn modified_unix_timestamp(path: &Path) -> Result<i64> {
    let metadata = std::fs::metadata(path)
        .with_context(|| format!("failed to read metadata for {}", path.display()))?;

    let modified = metadata
        .modified()
        .with_context(|| format!("failed to read modified timestamp for {}", path.display()))?;

    let since_epoch = modified.duration_since(UNIX_EPOCH).with_context(|| {
        format!(
            "modified timestamp for {} is before UNIX_EPOCH",
            path.display()
        )
    })?;

    Ok(since_epoch.as_secs() as i64)
}

pub fn modified_rfc3339(path: &Path) -> Result<String> {
    let metadata = std::fs::metadata(path)
        .with_context(|| format!("failed to read metadata for {}", path.display()))?;

    let modified = metadata
        .modified()
        .with_context(|| format!("failed to read modified timestamp for {}", path.display()))?;

    Ok(DateTime::<Utc>::from(modified).to_rfc3339())
}
