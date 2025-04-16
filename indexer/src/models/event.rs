use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

/// An optimized event representation for RabbitMQ publishing
/// Contains only essential data needed by downstream consumers
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EventMessage {
    pub id: String,
    pub tx_digest: String, 
    pub event_type: String,
    pub package_id: String,
    pub module: String,
    pub sender: String,
    pub timestamp: i64,
    pub checkpoint: i64,
    pub data: Value,
}

impl EventMessage {
    pub fn new(
        tx_digest: String,
        event_type: String,
        package_id: String,
        module: String,
        sender: String,
        timestamp: DateTime<Utc>,
        checkpoint: i64,
        data: Value,
    ) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            tx_digest,
            event_type,
            package_id,
            module,
            sender,
            timestamp: timestamp.timestamp_millis(),
            checkpoint,
            data,
        }
    }
}