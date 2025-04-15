use config::{Config, ConfigError, Environment, File};
use once_cell::sync::OnceCell;
use serde::Deserialize;
use std::sync::Arc;

static CONFIG: OnceCell<Arc<AppConfig>> = OnceCell::new();

#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseConfig {
    pub url: String,
    pub max_connections: u32,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SuiConfig {
    pub endpoint: String,
    pub initial_checkpoint: u64,
    pub concurrency: u32,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub database: DatabaseConfig,
    pub sui: SuiConfig,
    pub log_level: String,
}

impl AppConfig {
    pub fn load() -> Result<Self, ConfigError> {
        let config = Config::builder()
            .add_source(File::with_name("config/default").required(false))
            .add_source(File::with_name("config/local").required(false))
            .add_source(Environment::with_prefix("INDEXER").separator("__"))
            .build()?;

        config.try_deserialize()
    }

    pub fn global() -> Arc<AppConfig> {
        CONFIG
            .get_or_init(|| match Self::load() {
                Ok(config) => Arc::new(config),
                Err(e) => {
                    eprintln!("Failed to load config: {:?}", e);
                    Arc::new(Self::default())
                }
            })
            .clone()
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            database: DatabaseConfig {
                url: "postgres://postgres:postgres@localhost:5432/indexer".to_string(),
                max_connections: 5,
            },
            sui: SuiConfig {
                endpoint: "https://checkpoints.testnet.sui.io".to_string(),
                initial_checkpoint: 0,
                concurrency: 5,
            },
            log_level: "info".to_string(),
        }
    }
}
