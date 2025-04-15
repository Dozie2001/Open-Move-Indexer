mod config;
mod db;
mod error;
mod models;
mod processors;

use anyhow::Result;
use config::AppConfig;
use db::get_pool;
use processors::EventProcessor;
use sui_data_ingestion_core::setup_single_workflow;
use tracing::{Level, info};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<()> {
    // // Load environment variables from .env file
    // dotenv().ok();

    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("Failed to set tracing subscriber");

    info!("Starting Sui event indexer");

    let config = AppConfig::global();
    info!("Configuration loaded");

    let pool = get_pool();
    info!("Database connection established");

    db::migrate(&pool)
        .await
        .expect("Failed to run database migrations");

    let processor = EventProcessor::new(pool);

    info!("Setting up Sui data ingestion workflow");
    info!("Connecting to Sui node at: {}", config.sui.endpoint);
    info!(
        "Starting from checkpoint: {}",
        config.sui.initial_checkpoint
    );

    let (executor, _term_sender) = setup_single_workflow(
        processor,
        config.sui.endpoint.clone(),
        config.sui.initial_checkpoint,
        config.sui.concurrency as usize,
        None, /* extra reader options */
    )
    .await?;

    info!("Sui event indexer started successfully");

    executor.await?;

    Ok(())
}
