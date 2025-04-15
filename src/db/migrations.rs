use crate::error::Result;
use sqlx::{Executor, Pool, Postgres};
use tracing::info;

pub async fn migrate(pool: &Pool<Postgres>) -> Result<()> {
    info!("Running database migrations...");
    
    // Create events table
    pool.execute(
        r#"
        CREATE TABLE IF NOT EXISTS events (
            id UUID PRIMARY KEY,
            transaction_digest TEXT NOT NULL,
            event_type TEXT NOT NULL,
            package_id TEXT NOT NULL,
            module TEXT NOT NULL,
            function TEXT NOT NULL,
            object_id TEXT,
            object_type TEXT,
            sender TEXT NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            checkpoint_sequence_number BIGINT NOT NULL,
            event_sequence_number BIGINT NOT NULL,
            data JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        
        CREATE INDEX IF NOT EXISTS idx_events_transaction_digest ON events(transaction_digest);
        CREATE INDEX IF NOT EXISTS idx_events_event_type ON events(event_type);
        CREATE INDEX IF NOT EXISTS idx_events_package_id ON events(package_id);
        CREATE INDEX IF NOT EXISTS idx_events_module ON events(module);
        CREATE INDEX IF NOT EXISTS idx_events_checkpoint ON events(checkpoint_sequence_number);
        CREATE INDEX IF NOT EXISTS idx_events_object_id ON events(object_id);
        "#,
    )
    .await?;
    
    // Create processed_checkpoints table to track progress
    pool.execute(
        r#"
        CREATE TABLE IF NOT EXISTS processed_checkpoints (
            sequence_number BIGINT PRIMARY KEY,
            timestamp TIMESTAMPTZ NOT NULL,
            processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        "#,
    )
    .await?;
    
    info!("Database migrations completed successfully");
    
    Ok(())
}