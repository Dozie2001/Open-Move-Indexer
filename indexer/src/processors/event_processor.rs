use crate::error::Result;
use crate::models::EventMessage;
use crate::rabbitmq::RabbitMQConnection;
use anyhow::anyhow;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::sync::Arc;
use sui_data_ingestion_core::Worker;
use sui_types::full_checkpoint_content::CheckpointData;
use tracing::{debug, error, info, warn, instrument};
use tokio::time::{timeout, Duration};

const EVENT_PROCESSING_TIMEOUT_MS: u64 = 30000; // 30 seconds

pub struct EventProcessor {
    rabbitmq: Arc<RabbitMQConnection>,
    // Optional in-memory cache of recently processed checkpoints
    // to avoid duplicate processing in case of restarts or failures
    last_processed_checkpoint: std::sync::atomic::AtomicU64,
}

impl EventProcessor {
    pub fn new(rabbitmq: Arc<RabbitMQConnection>) -> Self {
        Self { 
            rabbitmq,
            last_processed_checkpoint: std::sync::atomic::AtomicU64::new(0),
        }
    }

    #[instrument(skip(self, checkpoint), fields(checkpoint_seq = checkpoint.checkpoint_summary.sequence_number))]
    async fn process_events(&self, checkpoint: &CheckpointData) -> Result<()> {
        let checkpoint_seq = checkpoint.checkpoint_summary.sequence_number;
        let last_processed = self.last_processed_checkpoint.load(std::sync::atomic::Ordering::Relaxed);

        if checkpoint_seq <= last_processed {
            debug!("Checkpoint {} already processed, skipping", checkpoint_seq);
            return Ok(());
        }
        
        let checkpoint_timestamp = checkpoint.checkpoint_summary.timestamp_ms;

        let checkpoint_datetime = DateTime::<Utc>::from_timestamp_millis(checkpoint_timestamp as i64)
            .ok_or_else(|| anyhow!("Invalid timestamp: {}", checkpoint_timestamp))?;

        info!("Processing events for checkpoint {}", checkpoint_seq);

        let estimated_event_count = checkpoint.transactions.len() * 2;  // rough estimate
        let mut events = Vec::with_capacity(estimated_event_count);
        
        let mut event_count = 0;
        
        match timeout(
            Duration::from_millis(EVENT_PROCESSING_TIMEOUT_MS),
            async {
                for transaction in checkpoint.transactions.iter() {            
                    let tx_digest = transaction.transaction.digest().base58_encode();
                    
                    for event_group in transaction.events.iter() {
                        for evt in event_group.data.iter() {
                            event_count += 1;
                            
                            let event_type = evt.type_.to_canonical_string(true);
                            let event_content = serde_json::to_value(&evt.contents)
                                .unwrap_or_else(|_| {
                                    warn!("Failed to serialize event contents for tx {}", tx_digest);
                                    Value::Null
                                });
                                
                            let event = EventMessage::new(
                                tx_digest.clone(),
                                event_type,
                                evt.package_id.to_hex(),
                                evt.transaction_module.to_string(),
                                evt.sender.to_string(),
                                checkpoint_datetime,
                                checkpoint_seq as i64,
                                event_content,
                            );
                
                            events.push(event);
                        }
                    }
                }
                
                Result::<()>::Ok(())
            }
        ).await {
            Ok(Ok(_)) => {},
            Ok(Err(e)) => {
                error!("Error extracting events from checkpoint {}: {}", checkpoint_seq, e);
                return Err(e.into());
            },
            Err(_) => {
                error!("Timeout extracting events from checkpoint {}", checkpoint_seq);
                return Err(anyhow!("Timeout extracting events from checkpoint {}", checkpoint_seq).into());
            }
        }

        if !events.is_empty() {
            match self.rabbitmq.publish_batch(&events).await {
                Ok(_) => {
                    info!("Published {} events to RabbitMQ for checkpoint {}", events.len(), checkpoint_seq);
                },
                Err(e) => {
                    error!("Failed to publish events for checkpoint {}: {}", checkpoint_seq, e);
                    return Err(e);
                }
            }
        } else {
            debug!("No events found in checkpoint {}", checkpoint_seq);
        }

        self.last_processed_checkpoint.store(checkpoint_seq, std::sync::atomic::Ordering::Relaxed);

        info!("Completed processing checkpoint {} with {} events", checkpoint_seq, event_count);

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