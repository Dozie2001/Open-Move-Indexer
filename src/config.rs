use anyhow::Result;
use serde::Deserialize;
use std::{fs, path::Path};

use config::{Config, File, FileFormat};

#[derive(Clone, Debug, Deserialize)]
pub struct IndexerConfig {
    pub worker_concurrency: usize,
    pub progress_store_file: String,
    pub reader_batch_size: Option<usize>,
    pub remote_store_url: Option<String>,
    pub cleanup_checkpoint_files: Option<bool>,
    pub starting_checkpoint_number: Option<u64>,
}

pub fn load_config_from_file(file_path: &str) -> Result<IndexerConfig> {
    let path = Path::new(file_path);
    let path_extension = path
        .extension()
        .map(|extension| extension.to_str().unwrap());

    if let Some(extension) = path_extension {
        let config_str = fs::read_to_string(path)?;
        let config_format = match extension {
            "toml" => FileFormat::Toml,
            "yaml" => FileFormat::Yaml,
            "json" => FileFormat::Json,
            _ => return Err(anyhow::anyhow!("Config file extension not supported")),
        };

        let config: IndexerConfig = Config::builder()
            .add_source(File::from_str(&config_str, config_format))
            .build()?
            .try_deserialize()?;
        return Ok(config);
    }

    Err(anyhow::anyhow!("Config file extension not supported"))
}
