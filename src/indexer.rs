use anyhow::{Ok, Result};
use async_trait::async_trait;
use sui_data_ingestion_core::Worker;
use sui_types::full_checkpoint_content::CheckpointData;

use crate::config::IndexerConfig;

#[derive(Clone, Debug)]
pub struct Indexer {
    pub current_checkpoint_number: u64,
    pub config: IndexerConfig,
}

impl Indexer {
    pub fn new(config: IndexerConfig) -> Self {
        Self {
            config,
            current_checkpoint_number: 0,
        }
    }
}

#[derive(Clone, Debug)]
pub struct IndexerWorker {
    indexer: Indexer,
}

impl IndexerWorker {
    pub fn new(indexer: Indexer) -> Self {
        Self { indexer }
    }
}

#[async_trait]
impl Worker for IndexerWorker {
    type Result = ();
    async fn process_checkpoint(&self, data: &CheckpointData) -> Result<()> {
        for transaction in &data.transactions {
            println!("Transaction Digest: {:?}", transaction.transaction.digest());
        }

        if data.checkpoint_summary.sequence_number % 100 == 0 {
            println!(
                "Checkpoint #{} processed",
                data.checkpoint_summary.sequence_number
            );
        }

        Ok(())
    }
}
