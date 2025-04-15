use crate::db::DbPool;
use crate::error::Result;
use crate::models::{Event, ProcessedCheckpoint};
use anyhow::anyhow;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::sync::Arc;
use sui_data_ingestion_core::Worker;
use sui_types::full_checkpoint_content::CheckpointData;
use tracing::{debug, info, warn};

pub struct EventProcessor {
    db_pool: Arc<DbPool>,
}

impl EventProcessor {
    pub fn new(db_pool: Arc<DbPool>) -> Self {
        Self { db_pool }
    }

    async fn process_events(&self, checkpoint: &CheckpointData) -> Result<()> {
        let checkpoint_seq = checkpoint.checkpoint_summary.sequence_number;
        let checkpoint_timestamp = checkpoint.checkpoint_summary.timestamp_ms;

        let checkpoint_timestamp = DateTime::<Utc>::from_timestamp(
            checkpoint_timestamp as i64 / 1000,
            0,
        )
        .ok_or(anyhow!("Invalid timestamp"))?;

        info!(
            "Processing events for checkpoint {}",
            checkpoint_seq
        );

        let mut events = Vec::new();
        let mut event_count = 0;

        for transaction in checkpoint.transactions.iter() {            
            let tx_digest = transaction.transaction.digest().base58_encode();
            let sender = transaction.transaction.sender_address();
            
            for (event_idx, event) in transaction.events.iter().enumerate() {
                
                event_count += 1;
                for evt in event.data.iter() {
                    let event_type = evt.type_.to_canonical_string(true);
                    let event_content  = serde_json::to_value(evt.contents.clone()).unwrap_or(Value::Null);
                        
                    // Create event record
                    let event = Event::new(
                        tx_digest.clone(),
                        event_type,
                        evt.package_id.to_hex(),
                        evt.transaction_module.to_string(),
                        evt.sender.to_string(),
                        checkpoint_timestamp,
                        checkpoint_seq as i64,
                        0,
                        event_content,
                    );
    
                    events.push(event);
                }
            }
        }

        if !events.is_empty() {
            Event::batch_insert(&events, self.db_pool.clone()).await?;
            info!("Inserted {} events for checkpoint {}", events.len(), checkpoint_seq);
        } else {
            debug!("No events found in checkpoint {}", checkpoint_seq);
        }

        let processed_checkpoint = ProcessedCheckpoint::new(checkpoint_seq as i64, checkpoint_timestamp);
        processed_checkpoint.insert(self.db_pool.clone()).await?;

        info!(
            "Completed processing checkpoint {} with {} events",
            checkpoint_seq, event_count
        );

        Ok(())
    }
}

#[async_trait]
impl Worker for EventProcessor {
    type Result = ();

    async fn process_checkpoint(&self, checkpoint: &CheckpointData) -> anyhow::Result<()> {
        match self.process_events(checkpoint).await {
            Ok(_) => Ok(()),
            Err(e) => {
                warn!("Error processing checkpoint: {:?}", e);
                Err(anyhow!("Failed to process checkpoint: {:?}", e))
            }
        }
    }
}