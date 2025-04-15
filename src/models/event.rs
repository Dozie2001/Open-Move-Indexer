use crate::db::DbPool;
use crate::error::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::postgres::PgQueryResult;
use uuid::Uuid;
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Event {
    pub id: Uuid,
    pub transaction_digest: String,
    pub event_type: String,
    pub package_id: String,
    pub module: String,
    pub sender: String,
    pub timestamp: DateTime<Utc>,
    pub checkpoint_sequence_number: i64,
    pub event_sequence_number: i64,
    pub data: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Event {
    pub fn new(
        transaction_digest: String,
        event_type: String,
        package_id: String,
        module: String,
        sender: String,
        timestamp: DateTime<Utc>,
        checkpoint_sequence_number: i64,
        event_sequence_number: i64,
        data: Value,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            transaction_digest,
            event_type,
            package_id,
            module,
            sender,
            timestamp,
            checkpoint_sequence_number,
            event_sequence_number,
            data,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    pub async fn insert(&self, pool: Arc<DbPool>) -> Result<PgQueryResult> {
        let result = sqlx::query!(
            r#"
            INSERT INTO events (
                id, transaction_digest, event_type, package_id, module, function,
                object_id, object_type, sender, timestamp, checkpoint_sequence_number,
                event_sequence_number, data, created_at, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            "#,
            self.id,
            self.transaction_digest,
            self.event_type,
            self.package_id,
            self.module,
            self.function,
            self.object_id,
            self.object_type,
            self.sender,
            self.timestamp,
            self.checkpoint_sequence_number,
            self.event_sequence_number,
            self.data,
            self.created_at,
            self.updated_at
        )
        .execute(pool.as_ref())
        .await?;

        Ok(result)
    }

    pub async fn batch_insert(events: &[Event], pool: Arc<DbPool>) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        let mut tx = pool.begin().await?;

        for event in events {
            sqlx::query!(
                r#"
                INSERT INTO events (
                    id, transaction_digest, event_type, package_id, module, function,
                    object_id, object_type, sender, timestamp, checkpoint_sequence_number,
                    event_sequence_number, data, created_at, updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                "#,
                event.id,
                event.transaction_digest,
                event.event_type,
                event.package_id,
                event.module,
                event.function,
                event.object_id,
                event.object_type,
                event.sender,
                event.timestamp,
                event.checkpoint_sequence_number,
                event.event_sequence_number,
                event.data,
                event.created_at,
                event.updated_at
            )
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;

        Ok(())
    }

    pub async fn find_by_package_and_module(
        package_id: &str,
        module: &str,
        limit: i64,
        pool: Arc<DbPool>,
    ) -> Result<Vec<Event>> {
        let events = sqlx::query_as!(
            Event,
            r#"
            SELECT 
                id, transaction_digest, event_type, package_id, module, function,
                object_id, object_type, sender, timestamp, checkpoint_sequence_number,
                event_sequence_number, data, created_at, updated_at
            FROM events
            WHERE package_id = $1 AND module = $2
            ORDER BY checkpoint_sequence_number DESC, event_sequence_number DESC
            LIMIT $3
            "#,
            package_id,
            module,
            limit
        )
        .fetch_all(pool.as_ref())
        .await?;

        Ok(events)
    }

    pub async fn find_by_event_type(
        event_type: &str,
        limit: i64,
        pool: Arc<DbPool>,
    ) -> Result<Vec<Event>> {
        let events = sqlx::query_as!(
            Event,
            r#"
            SELECT 
                id, transaction_digest, event_type, package_id, module, function,
                object_id, object_type, sender, timestamp, checkpoint_sequence_number,
                event_sequence_number, data, created_at, updated_at
            FROM events
            WHERE event_type = $1
            ORDER BY checkpoint_sequence_number DESC, event_sequence_number DESC
            LIMIT $2
            "#,
            event_type,
            limit
        )
        .fetch_all(pool.as_ref())
        .await?;

        Ok(events)
    }
}