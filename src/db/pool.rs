use crate::config::AppConfig;
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::sync::Arc;
use once_cell::sync::OnceCell;

pub type DbPool = PgPool;
static DB_POOL: OnceCell<Arc<DbPool>> = OnceCell::new();

pub async fn create_pool(config: &AppConfig) -> Result<DbPool, sqlx::Error> {
    let pool = PgPoolOptions::new()
        .max_connections(config.database.max_connections)
        .connect(&config.database.url)
        .await?;
    
    Ok(pool)
}

pub async fn initialize_pool() -> Result<Arc<DbPool>, sqlx::Error> {
    let config = AppConfig::global();
    let pool = create_pool(&config).await?;
    
    Ok(Arc::new(pool))
}

pub fn get_pool() -> Arc<DbPool> {
    DB_POOL
        .get_or_init(|| {
            let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
            rt.block_on(async {
                initialize_pool()
                    .await
                    .expect("Failed to initialize database pool")
            })
        })
        .clone()
}