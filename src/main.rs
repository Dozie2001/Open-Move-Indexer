use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use config::IndexerConfig;
use prometheus::Registry;
use serde::Deserialize;
use sui_data_ingestion_core::{
    DataIngestionMetrics, FileProgressStore, IndexerExecutor, ProgressStore, ReaderOptions,
    ShimProgressStore, WorkerPool,
};
use tokio::sync::oneshot;

use indexer::{Indexer, IndexerWorker};

mod config;
mod indexer;

#[derive(Debug, Deserialize, Parser)]
#[command(author, version, about, long_about = None)]
struct IndexerArgs {
    #[arg(long)]
    config_file: String,
}

const WORKER_NAME: &str = "sui_webhook_indexer";

#[tokio::main]
async fn main() -> Result<()> {
    let args = IndexerArgs::parse();
    let config = config::load_config_from_file(&args.config_file)?;

    let indexer = Indexer::new(config.clone());
    let (exit_sender, exit_receiver) = oneshot::channel();
    let metrics = DataIngestionMetrics::new(&Registry::new());
    let progress_store = FileProgressStore::new(PathBuf::from(config.progress_store_file));
    let mut executor = IndexerExecutor::new(progress_store, 1, metrics);

    let worker_pool = WorkerPool::new(
        IndexerWorker::new(indexer),
        WORKER_NAME.to_string(),
        config.worker_concurrency,
    );

    let reader_options = ReaderOptions {
        batch_size: config.reader_batch_size.unwrap_or(100),
        gc_checkpoint_files: config.cleanup_checkpoint_files.unwrap_or(false),
        ..Default::default()
    };

    executor.register(worker_pool).await?;
    executor
        .run(
            PathBuf::from(""),
            config.remote_store_url,
            vec![],
            reader_options,
            exit_receiver,
        )
        .await?;
    Ok(())
}