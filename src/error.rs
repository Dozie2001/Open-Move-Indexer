use thiserror::Error;
use anyhow::Error as AnyhowError;

#[derive(Error, Debug)]
pub enum IndexerError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Sui processing error: {0}")]
    SuiError(String),

    #[error("Config error: {0}")]
    ConfigError(#[from] config::ConfigError),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    SerdeError(#[from] serde_json::Error),

    #[error("Anyhow error: {0}")]
    AnyhowError(#[from] AnyhowError),

    #[error("Unknown error: {0}")]
    Unknown(String),
}

pub type Result<T> = std::result::Result<T, IndexerError>;
