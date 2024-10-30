use aws_sdk_kinesis::{
    Client as KinesisClient, 
    config as KinesisConfig,
    config::Region,
};
use aws_sdk_sqs::{Client as SQSClient, config as SQSConfig};
use crate::modules::{
    shard_data_store::ShardDataStore,
    submission_worker::SubmissionWorker,
    traffic_monitor::TrafficMonitor,
};
use tokio::{time, sync::RwLock};
use std::sync::Arc;

mod modules;

#[tokio::main]
async fn main() {
    // Build the configuration for SQS
    let sqs_config = SQSConfig::Builder::new()
        .region(Region::new("eu-west-1"))
        .endpoint_url("http://localhost:4566/000000000000/submissions") // Queue URL: Differs from 'list-queues' URL, that one gives error
        .credentials_provider(SQSConfig::Credentials::new(
            "some_key_id",      // Access key for LocalStack
            "some_secret",  // Secret key for LocalStack
            None,               // Optional session token
            None,               // Expiry (optional)
            "localstack",       // Provider name
        ))
        .build();

    // Construct the SQS client based on our configuration
    let sqs_client = SQSClient::from_conf(sqs_config);

    // Build the configuration for Kinesis
    let kinesis_config = KinesisConfig::Builder::new()
        .region(Region::new("eu-west-1"))
        .endpoint_url("http://localhost:4566/") // Kinesis uses the root URL
        .credentials_provider(KinesisConfig::Credentials::new(
            "some_key_id",      // Access key for LocalStack
            "some_secret",  // Secret key for LocalStack
            None,               // Optional session token
            None,               // Expiry (optional)
            "localstack",       // Provider name
        ))
        .build();

    // Construct the Kinesis client based on our configuration
    let kinesis_client = KinesisClient::from_conf(kinesis_config);

    // Create a new, shared ShardDataStore instance using RWLock
    let shard_data = Arc::new(RwLock::new(ShardDataStore::new()));


    // Create a new TrafficMonitor instance
    let mut monitor = TrafficMonitor::new(kinesis_client.clone(), time::Duration::from_secs(3), shard_data.clone());

    // Start monitoring traffic
    tokio::spawn(async move {
        monitor.start_monitoring().await;
    });

    // Create a new SubmissionWorker instance
    let worker = SubmissionWorker::new(sqs_client, kinesis_client, shard_data.clone());

    // Run the worker
    worker.run().await;
}