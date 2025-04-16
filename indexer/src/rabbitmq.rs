use crate::error::Result;
use anyhow::anyhow;
use lapin::{
    options::{
        BasicPublishOptions, QueueDeclareOptions, ExchangeDeclareOptions, 
         BasicQosOptions
    },
    publisher_confirm::Confirmation,
    types::FieldTable,
    BasicProperties, Connection, ConnectionProperties, Channel, ExchangeKind
};
use serde::Serialize;
use std::{sync::Arc, time::Duration};
use tokio::sync::{Mutex, Semaphore};
use tracing::{debug, error, info, warn};
use tokio::time::timeout;

const CONNECTION_RETRY_DELAY_MS: u64 = 5000;
const CONNECTION_TIMEOUT_MS: u64 = 10000;
const MAX_CONCURRENT_PUBLISHES: usize = 100;
const PUBLISH_TIMEOUT_MS: u64 = 5000;
const CHANNEL_PREFETCH_COUNT: u16 = 100;

pub struct RabbitMQConnection {
    channel: Arc<Mutex<Channel>>,
    connection: Arc<Connection>,
    queue_name: String,
    exchange_name: Option<String>,
    routing_key: String,
    publish_semaphore: Arc<Semaphore>,
    batch_size: usize,
}

impl RabbitMQConnection {
    pub async fn new(
        amqp_url: &str, 
        queue_name: &str,
        exchange_name: Option<&str>,
        routing_key: Option<&str>,
        batch_size: Option<usize>,
    ) -> Result<Self> {
        info!("Connecting to RabbitMQ at {}", amqp_url);
        
        let connection = Self::establish_connection(amqp_url).await?;
        let channel = Self::create_channel(&connection).await?;
        
        channel.basic_qos(
            CHANNEL_PREFETCH_COUNT,
            BasicQosOptions::default()
        ).await.map_err(|e| anyhow!("Failed to set channel QoS: {}", e))?;
        
        if let Some(exchange) = exchange_name {
            Self::setup_exchange(&channel, exchange).await?;
        }
        
        Self::setup_queue(&channel, queue_name).await?;
        
        if let Some(exchange) = exchange_name {
            let routing = routing_key.unwrap_or(queue_name);
            Self::bind_queue_to_exchange(&channel, queue_name, exchange, routing).await?;
        }
        
        let effective_batch_size = batch_size.unwrap_or(100);
        
        info!("Successfully connected to RabbitMQ with queue {}", queue_name);
        
        Ok(Self {
            channel: Arc::new(Mutex::new(channel)),
            connection: Arc::new(connection),
            queue_name: queue_name.to_string(),
            exchange_name: exchange_name.map(|s| s.to_string()),
            routing_key: routing_key.unwrap_or(queue_name).to_string(),
            publish_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_PUBLISHES)),
            batch_size: effective_batch_size,
        })
    }
    
    async fn establish_connection(amqp_url: &str) -> Result<Connection> {
        let connection_props = ConnectionProperties::default()
            .with_connection_name("sui_indexer".into());
        
        let mut retry_count = 0;
        let max_retries = 5;
        
        loop {
            match timeout(
                Duration::from_millis(CONNECTION_TIMEOUT_MS),
                Connection::connect(amqp_url, connection_props.clone())
            ).await {
                Ok(Ok(conn)) => return Ok(conn),
                Ok(Err(e)) => {
                    retry_count += 1;
                    error!("Failed to connect to RabbitMQ (attempt {}/{}): {}", 
                        retry_count, max_retries, e);
                    
                    if retry_count >= max_retries {
                        return Err(anyhow!("Failed to connect to RabbitMQ after {} attempts: {}", 
                            max_retries, e).into());
                    }
                },
                Err(_) => {
                    retry_count += 1;
                    error!("Connection to RabbitMQ timed out (attempt {}/{})", 
                        retry_count, max_retries);
                        
                    if retry_count >= max_retries {
                        return Err(anyhow!("Failed to connect to RabbitMQ after {} attempts: connection timeout", 
                            max_retries).into());
                    }
                }
            }
            
            tokio::time::sleep(Duration::from_millis(CONNECTION_RETRY_DELAY_MS)).await;
        }
    }
    
    async fn create_channel(connection: &Connection) -> Result<Channel> {
        connection.create_channel().await
            .map_err(|e| anyhow!("Failed to create RabbitMQ channel: {}", e).into())
    }
    
    async fn setup_exchange(channel: &Channel, exchange_name: &str) -> Result<()> {
        channel.exchange_declare(
            exchange_name,
            ExchangeKind::Topic, 
            ExchangeDeclareOptions {
                durable: true,
                ..ExchangeDeclareOptions::default()
            },
            FieldTable::default(),
        ).await.map_err(|e| anyhow!("Failed to declare exchange {}: {}", exchange_name, e))?;
        
        Ok(())
    }
    
    async fn setup_queue(channel: &Channel, queue_name: &str) -> Result<()> {
        channel.queue_declare(
            queue_name,
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default()
                .into(), 
        ).await.map_err(|e| anyhow!("Failed to declare queue {}: {}", queue_name, e))?;
        
        Ok(())
    }
    
    async fn bind_queue_to_exchange(
        channel: &Channel, 
        queue_name: &str,
        exchange_name: &str,
        routing_key: &str
    ) -> Result<()> {
        channel.queue_bind(
            queue_name,
            exchange_name,
            routing_key,
            Default::default(),
            FieldTable::default(),
        ).await.map_err(|e| anyhow!(
            "Failed to bind queue {} to exchange {} with routing key {}: {}", 
            queue_name, exchange_name, routing_key, e
        ))?;
        
        Ok(())
    }
    
    pub async fn publish<T: Serialize>(&self, message: &T) -> Result<()> {
        let _permit = self.publish_semaphore.acquire().await
            .map_err(|e| anyhow!("Failed to acquire publish permit: {}", e))?;
            
        let payload = serde_json::to_vec(message)
            .map_err(|e| anyhow!("Failed to serialize message: {}", e))?;
        
        let channel = self.channel.lock().await;
        
        let (exchange, routing_key) = match &self.exchange_name {
            Some(exchange) => (exchange.as_str(), self.routing_key.as_str()),
            None => ("", self.queue_name.as_str()),  
        };
        
        debug!("Publishing message to exchange: '{}', routing key: '{}'", 
            if exchange.is_empty() { "default" } else { exchange }, 
            routing_key);
        

        let properties = BasicProperties::default()
            .with_delivery_mode(2) 
            .with_content_type("application/json".into());
        
        match timeout(
            Duration::from_millis(PUBLISH_TIMEOUT_MS),
            channel.basic_publish(
                exchange,
                routing_key,
                BasicPublishOptions::default(),
                &payload,
                properties,
            )
        ).await {
            Ok(Ok(confirm_future)) => {
                match timeout(
                    Duration::from_millis(PUBLISH_TIMEOUT_MS),
                    confirm_future
                ).await {
                    Ok(Ok(confirm)) => {
                        match confirm {
                            Confirmation::Ack(_) => {
                                debug!("Message successfully published");
                                Ok(())
                            }
                            Confirmation::Nack(_) => {
                                error!("Message rejected by RabbitMQ");
                                Err(anyhow!("Message rejected by RabbitMQ broker").into())
                            }
                            Confirmation::NotRequested => {
                                debug!("Message published (no confirmation requested)");
                                Ok(())
                            }
                        }
                    },
                    Ok(Err(e)) => {
                        error!("Failed to get confirmation for message: {}", e);
                        Err(anyhow!("Failed to get publish confirmation: {}", e).into())
                    },
                    Err(_) => {
                        error!("Publish confirmation timed out");
                        Err(anyhow!("Publish confirmation timed out").into())
                    }
                }
            },
            Ok(Err(e)) => {
                error!("Failed to publish message: {}", e);
                Err(anyhow!("Failed to publish message: {}", e).into())
            },
            Err(_) => {
                error!("Publish operation timed out");
                Err(anyhow!("Publish operation timed out").into())
            }
        }
    }
    
    pub async fn publish_batch<T: Serialize>(&self, messages: &[T]) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }
        
        let batch_size = self.batch_size;
        
        for chunk in messages.chunks(batch_size) {
            let mut tasks = Vec::with_capacity(chunk.len());
            
            for message in chunk {
                let this = self.clone();
                let message_clone = serde_json::to_vec(message)
                    .map_err(|e| anyhow!("Failed to serialize message: {}", e))?;
                
                tasks.push(tokio::spawn(async move {
                    let result: Result<()> = (|| async {
                        let _permit = this.publish_semaphore.acquire().await
                            .map_err(|e| anyhow!("Failed to acquire publish permit: {}", e))?;
                            
                        let channel = this.channel.lock().await;
                        
                        let (exchange, routing_key) = match &this.exchange_name {
                            Some(exchange) => (exchange.as_str(), this.routing_key.as_str()),
                            None => ("", this.queue_name.as_str()),
                        };
                        
                        let properties = BasicProperties::default()
                            .with_delivery_mode(2) 
                            .with_content_type("application/json".into());
                        
                        match timeout(
                            Duration::from_millis(PUBLISH_TIMEOUT_MS),
                            channel.basic_publish(
                                exchange,
                                routing_key,
                                BasicPublishOptions::default(),
                                &message_clone,
                                properties,
                            )
                        ).await {
                            Ok(Ok(confirm_future)) => {
                                match timeout(
                                    Duration::from_millis(PUBLISH_TIMEOUT_MS),
                                    confirm_future
                                ).await {
                                    Ok(Ok(confirm)) => {
                                        match confirm {
                                            Confirmation::Ack(_) => Ok(()),
                                            Confirmation::Nack(_) => {
                                                Err(anyhow!("Message rejected by RabbitMQ broker").into())
                                            }
                                            Confirmation::NotRequested => Ok(()),
                                        }
                                    },
                                    Ok(Err(e)) => {
                                        Err(anyhow!("Failed to get publish confirmation: {}", e).into())
                                    },
                                    Err(_) => {
                                        Err(anyhow!("Publish confirmation timed out").into())
                                    }
                                }
                            },
                            Ok(Err(e)) => {
                                Err(anyhow!("Failed to publish message: {}", e).into())
                            },
                            Err(_) => {
                                Err(anyhow!("Publish operation timed out").into())
                            }
                        }
                    })().await;
                    
                    result
                }));
            }
            
            let mut has_errors = false;
            for task in tasks {
                match task.await {
                    Ok(Ok(_)) => {}, 
                    Ok(Err(e)) => {
                        error!("Failed to publish message in batch: {}", e);
                        has_errors = true;
                    },
                    Err(e) => {
                        error!("Task failed during batch publish: {}", e);
                        has_errors = true;
                    }
                }
            }
            
            if has_errors {
                return Err(anyhow!("One or more messages failed to publish").into());
            }
        }
        
        Ok(())
    }
    

    pub async fn shutdown(&self) -> Result<()> {
        let channel = self.channel.lock().await;
        if let Err(e) = channel.close(0, "Shutdown requested").await {
            warn!("Error closing RabbitMQ channel: {}", e);
        }
        
        if let Err(e) = self.connection.close(0, "Shutdown requested").await {
            warn!("Error closing RabbitMQ connection: {}", e);
        }
        
        Ok(())
    }
}

impl Clone for RabbitMQConnection {
    fn clone(&self) -> Self {
        Self {
            channel: self.channel.clone(),
            connection: self.connection.clone(),
            queue_name: self.queue_name.clone(),
            exchange_name: self.exchange_name.clone(),
            routing_key: self.routing_key.clone(),
            publish_semaphore: self.publish_semaphore.clone(),
            batch_size: self.batch_size,
        }
    }
}

impl Drop for RabbitMQConnection {
    fn drop(&mut self) {
        // Nothing to do here as we're using Arc for the connection
        // The actual shutdown should be handled explicitly via shutdown()
    }
}