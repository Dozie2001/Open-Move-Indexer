mod config;
mod error;
mod processors;
mod rabbitmq;
mod models;

use anyhow::Result;
use config::AppConfig;
use processors::EventProcessor;
use rabbitmq::RabbitMQConnection;
use sui_data_ingestion_core::setup_single_workflow;
use std::sync::Arc;
use tracing::{Level, info, error};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("Failed to set tracing subscriber");

    info!("Starting Sui event indexer");

    let config = AppConfig::global();
    info!("Configuration loaded");

    let rabbitmq = Arc::new(
        RabbitMQConnection::new(
            &config.rabbitmq.url, 
            &config.rabbitmq.queue,
            config.rabbitmq.exchange.as_deref(),
            config.rabbitmq.routing_key.as_deref(),
            config.rabbitmq.batch_size
        )
        .await
        .expect("Failed to connect to RabbitMQ")
    );
    info!("RabbitMQ connection established");

    let processor = EventProcessor::new(rabbitmq.clone());

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

    let rabbitmq_shutdown = rabbitmq.clone();
    
    tokio::spawn(async move {
        let ctrl_c = tokio::signal::ctrl_c();
        
        #[cfg(unix)]
        let terminate = async {
            use tokio::signal::unix::{signal, SignalKind};
            signal(SignalKind::terminate())
                .expect("failed to install SIGTERM handler")
                .recv()
                .await;
        };

        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

        tokio::select! {
            _ = ctrl_c => info!("Received SIGINT, shutting down"),
            _ = terminate => info!("Received SIGTERM, shutting down"),
        }

        info!("Closing RabbitMQ connection...");
        if let Err(e) = rabbitmq_shutdown.shutdown().await {
            error!("Failed to close RabbitMQ connection: {}", e);
        }
    });

    let result = executor.await?;
    
    info!("Sui event indexer shutting down");
    
    Ok(())
}
