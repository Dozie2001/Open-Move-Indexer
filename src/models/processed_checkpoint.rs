use crate::db::DbPool;
use crate::error::Result;
use chrono::{DateTime, Utc};
use sqlx::postgres::PgQueryResult;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ProcessedCheckpoint {
    pub sequence_number: i64,
    pub timestamp: DateTime<Utc>,
    pub processed_at: DateTime<Utc>,
}

impl ProcessedCheckpoint {
    pub fn new(sequence_number: i64, timestamp: DateTime<Utc>) -> Self {
        Self {
            sequence_number,
            timestamp,
            processed_at: Utc::now(),
        }
    }

    pub async fn insert(&self, pool: Arc<DbPool>) -> Result<PgQueryResult> {
        let result = sqlx::query!(
            r#"
            INSERT INTO processed_checkpoints (sequence_number, timestamp, processed_at)
            VALUES ($1, $2, $3)
            ON CONFLICT (sequence_number) DO NOTHING
            "#,
            self.sequence_number,
            self.timestamp,
            self.processed_at
        )
        .execute(pool.as_ref())
        .await?;

        Ok(result)
    }

    pub async fn get_latest(pool: Arc<DbPool>) -> Result<Option<ProcessedCheckpoint>> {
        let checkpoint = sqlx::query_as!(
            ProcessedCheckpoint,
            r#"
            SELECT sequence_number, timestamp, processed_at
            FROM processed_checkpoints
            ORDER BY sequence_number DESC
            LIMIT 1
            "#
        )
        .fetch_optional(pool.as_ref())
        .await?;

        Ok(checkpoint)
    }
}