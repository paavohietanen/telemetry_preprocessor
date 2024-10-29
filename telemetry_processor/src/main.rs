use aws_sdk_kinesis::{Client as KinesisClient, config as KinesisConfig, config::Region, types::{Shard, ShardIteratorType}};
use aws_sdk_sqs::types::Message;
use aws_sdk_sqs::{Client as SQSClient, config as SQSConfig};
use base64::{self, Engine, engine::general_purpose};
use futures;
use jsonschema;
use serde::{Serialize, Deserialize};
use std::fmt;
use tokio::time::sleep;
use tokio::{task, time, sync::RwLock};
use std::{
    sync::Arc,
    time::SystemTime
};

const MAX_CAPACITY: f64 = 1_000_000.0; // 1 MB/s capacity of a shard
const SPLIT_THRESHOLD: f64 = 0.8; // 80% threshold
const MERGE_THRESHOLD: f64 = 0.3; // 30% threshold
const MONITORING_DURATION: u64 = 10; // Last 10 seconds for monitoring
const ENTRY_LIFETIME: std::time::Duration = std::time::Duration::from_secs(60); // 60 seconds

// ShardMetrics struct to hold metrics for each shard
#[derive(Debug, Clone)]
struct ShardMetrics {
    shard_id: String,
    average_data_processed: f64,
    entries: Vec<ShardMetricEntry>,
}

// A single entry of shard data
#[derive(Debug, Clone)]
struct ShardMetricEntry {
    timestamp: SystemTime,
    data_processed: u64,
    records_read: u64,
}

struct ShardDataStore {
    shards: Vec<ShardMetrics>,
}

impl ShardDataStore {
    pub fn new() -> Self {
        Self {
            shards: Vec::new(),
        }
    }

    // Function to fetch shards and create ShardMetrics based on them
    pub async fn init(&mut self, shard_ids: Vec<String>) {

        // Create ShardMetrics for each shard
        for shard_id in shard_ids {
            self.shards.push(ShardMetrics {
                shard_id,
                average_data_processed: 0.0,
                entries: Vec::new(),
            });
        }
    }

    // Init a single new shard
    pub fn add_shard(&mut self, shard_id: String) {
        self.shards.push(ShardMetrics {
            shard_id,
            average_data_processed: 0.0,
            entries: Vec::new(),
        });
    }

    // Function to record metrics data for the traffic monitor
    // Currently only stores written bytes and records
    pub async fn record_metrics(&mut self, shard_id: &str, data_size: u64, record_count: u64) {
        // Find the shard in the shards vector
        let shard = self.shards.iter_mut().find(|s| s.shard_id == shard_id);

        // If the shard is found, update the metrics
        if let Some(shard) = shard {
            shard.entries.push(ShardMetricEntry {
                timestamp: SystemTime::now(),
                data_processed: data_size,
                records_read: record_count,
            });
        } else {
            // If the shard is not found, create a new entry
            self.shards.push(ShardMetrics {
                shard_id: shard_id.to_string(),
                average_data_processed: 0.0,
                entries: vec![ShardMetricEntry {
                    timestamp: SystemTime::now(),
                    data_processed: data_size,
                    records_read: record_count,
                }],
            });
        }        
    }
           
}

struct TrafficMonitor {
    kinesis_client: aws_sdk_kinesis::Client,
    high_traffic_threshold: f64, // e.g., 0.8 for 80%
    low_traffic_threshold: f64, // e.g., 0.3 for 30%
    check_interval: tokio::time::Duration,
    // A shared resource between Monitor and Workers
    shard_data: Arc<RwLock<ShardDataStore>>,
}

impl TrafficMonitor {

    pub fn new(kinesis_client: aws_sdk_kinesis::Client, check_interval: tokio::time::Duration, shard_data: Arc<RwLock<ShardDataStore>>) -> Self {
        Self {
            kinesis_client,
            high_traffic_threshold: 0.8,
            low_traffic_threshold: 0.3,
            check_interval,
            shard_data,
        }
    }

    // Function to start monitoring traffic
    pub async fn start_monitoring(&mut self) {

        // First initiate the shard data storage
        self.init_shard_data().await;

        // Start the monitoring loop
        println!("Starting traffic monitoring...");
        loop {
            // Check traffic conditions
            self.manage_traffic().await;
            tokio::time::sleep(self.check_interval).await;
        };
    }

    // Init the shard data storage
    pub async fn init_shard_data(&mut self) {
        // Fetch all shards from the Kinesis stream
        let shard_ids = self.fetch_shards().await;

        // Acquire a write lock on `shard_data`
        let mut shard_data = self.shard_data.write().await;

        // Create ShardMetrics for each shard
        shard_data.init(shard_ids).await;
    }

    // Fetch all shards from the Kinesis stream
    async fn fetch_shards(&self) -> Vec<String> {
        // Initiate a new list for shard IDs
        let mut shard_ids = Vec::new();

        // Kinesis might return the shards in multiple responses
        // To get all subsets of shards, we need a marker from where to start
        // retrieving the next batch of shards, if any left.
        // Initialize this first to None, to start from the beginning
        let mut next_token: Option<String> = None;

        // Loop to handle paginated results
        loop {
            // Describe the stream to get shard information
            let mut request = self.kinesis_client.describe_stream()
            .stream_name("events"); // Stream name

            // Only add exclusive_start_shard_id if next_token is Some
            if let Some(ref token) = next_token {
                request = request.exclusive_start_shard_id(token); // Use token directly
            }

            // Send the request and await the response
            let response = request.send().await;

            match response {
                Ok(ref res) => {
                    if let Some(ref description) = res.stream_description {
                        // Append the shard IDs to the list
                        for shard in &description.shards {
                            shard_ids.push(shard.shard_id.clone());
                        }
                        
                        // Function .last() returns the last element of the iterator, or None, if there are no elements
                        // If there is a last shard, map() takes the last shard `s` and returns the clone of its shard_id
                        next_token = description.shards.last().map(|s| s.shard_id.clone());
                    }

                    // Break the loop if there are no more shards to process
                    if next_token.is_none() {
                        break;
                    }
                }
                Err(err) => {
                    println!("Error fetching shards: {:?}", err);
                    break; // Handle the error as appropriate for your application
                }
            }
        }

        shard_ids
    }

    
    // Function to check traffic and manage shards
    pub async fn manage_traffic(&mut self) {
        let current_time = SystemTime::now();

        // Acquire a write lock on `shard_data`
        let mut shard_data = self.shard_data.write().await;

        // Iterate through shards
        for mut metrics in shard_data.shards.clone() {

            // Remove entries older than ENTRY_LIFETIME
            metrics.entries.retain(|entry| {
                current_time.duration_since(entry.timestamp).map_or(false, |elapsed| {
                    elapsed <= ENTRY_LIFETIME
                })
            });
            // Calculate the average data processed per second from all entries that
            // fall to the monitoring duration
            let mut total_data_processed: f64 = 0.0;
            let mut number_of_entries: u64 = 0;
            // Iterate through shard metrics
            for entry in &metrics.entries {
            
                // Check if the metrics were recorded in the last 10 seconds
                if let Ok(elapsed) = current_time.duration_since(entry.timestamp) {

                    if elapsed.as_secs() <= MONITORING_DURATION {
                        // Add the data processed to the total
                        total_data_processed += entry.data_processed as f64;
                        number_of_entries += 1;
                        // Calculate the percentage of capacity used
                        let usage_percentage = (entry.data_processed as f64) / MAX_CAPACITY * 100.0;

                        // If the usage percentage exceeds the threshold, split the shard
                        if usage_percentage >= SPLIT_THRESHOLD * 100.0 {
                            println!("Shard {} is over 80% capacity, splitting...", metrics.shard_id);
                            //self.split_shard(&metrics.shard_id).await;
                        }

                        // If the usage percentage is below or equal to the merge threshold, merge the shard
                        if usage_percentage <= MERGE_THRESHOLD * 100.0 {
                            println!("Shard {} is below or equal to 30% capacity, merging...", metrics.shard_id);
                            //self.merge_shard(&metrics.shard_id).await; // Assuming you have a merge_shard method
                        }
                    }
                }
            }
            metrics.average_data_processed = total_data_processed / number_of_entries as f64;
            println!(" ========= Shard {} average data processed: {}", metrics.shard_id, metrics.average_data_processed);
        }
    }
}

/*    async fn split_shard(&mut self, shard_id: &String) {
        // Use the Kinesis client to split the shard
        // Tie the new shard to a given UUID
    
        let split_result = self.kinesis_client
            .split_shard()
            .stream_name("events")
            .shard_to_split(shard_id.clone())
            .new_starting_hash_key("new_hash_key") // Define your hash key
            .send()
            .await;
    
        match split_result {
            Ok(_) => println!("Successfully split shard: {:?}", shard_id),
            Err(err) => println!("Error splitting shard: {:?}", err),
        }
    }
    
    async fn merge_shard(&mut self, shard_id: &String) {
        // Use the Kinesis client to merge the shard with its adjacent shard
    
        let merge_result = self.kinesis_client
            .merge_shards()
            .stream_name("your_stream_name")
            .shard_to_merge(shard.shard_id.clone())
            .adjacent_shard_to_merge("adjacent_shard_id") // Define your adjacent shard
            .send()
            .await;
    
        match merge_result {
            Ok(_) => println!("Successfully merged shard: {:?}", shard_id),
            Err(err) => println!("Error merging shard: {:?}", err),
        }
    }
} */

// Custom WorkerError to better control error handling at the point of task spawning
// e.g. not all errors implement `Send` trait, so they can't be returned from a task
#[derive(Debug, Clone)]
pub enum WorkerError {
    // Error for writing to Kinesis, contains a list of errors due to writing retry
    WritingError(Vec::<String>),
    // Error for JSON deserialization
    JsonDeserializationError(String),
    // Error for JSON serialization
    JsonSerializationError(String),
    // Error for submission validation
    // Refrain from using ValidationError from jsonschema crate in order to not have to
    // deal with the lifetime specifier in the error type
    ValidationError(String),
    // Error for no body found in submission
    NoBodyError(String),

}

// Implement Display for WorkerError
impl fmt::Display for WorkerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WorkerError::WritingError(err) => write!(f, "Writing error: {}", err.join(", ")),
            WorkerError::JsonDeserializationError(err) => write!(f, "JSON deserialization error: {}", err),
            WorkerError::JsonSerializationError(err) => write!(f, "JSON serialization error: {}", err),
            WorkerError::ValidationError(err) => write!(f, "Validation error: {}", err),
            WorkerError::NoBodyError(err) => write!(f, "No body error: {}", err),
        }
    }
}

impl std::error::Error for WorkerError {}

#[derive(Debug, Serialize, Deserialize)]
struct Submission {
    // Unique identifier for the submission
    submission_id: String,
    // Unique identifier for the device
    device_id: String,
    // Creation time of the submission, device local time
    time_created: String,
    // Corresponding event data
    events: Event,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Event {
    // List of new process events
    new_process: Vec<NewProcess>,
    // List of network connection events
    network_connection: Vec<NetworkConnection>
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NewProcess {
    // Command line of the process
    cmdl: String,
    // Username that started the process
    user: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NetworkConnection {
    // IPv4
    source_ip: String,
    // IpV4
    destination_ip: String,
    // Range of 0-65535
    destination_port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum EventType {
    // Event type for new process
    NewProcess(NewProcess),
    // Event type for network connection
    NetworkConnection(NetworkConnection),
}

// EventWrapper struct to hold single processed events
#[derive(Debug, Serialize, Deserialize)]
struct EventWrapper {
    // Event type with its corresponding data
    event_type: EventType,
    // Unique identifier of the event
    event_id: String,
    // Unique identifier for the submission event belongs to
    submission_id: String,
    // Order number in submission
    order: u32,
    // Creation time of the submission, device local time
    time_created: String,
    // Processing time of the event, application local time
}

// Helper function to decode base64 binary data
fn decode_base64(encoded_data: &str) -> Result<Vec<u8>, base64::DecodeError> {
    general_purpose::STANDARD.decode(encoded_data)
}

// Helper function to validate JSON against a schema
async fn is_valid_submission(json_str: &str) -> Result<(), WorkerError> {

    // Initiate the control schema
    let control_schema = serde_json::json!({
        "type": "object",
        "properties": {
            "submission_id": { "type": "string" },
            "device_id": { "type": "string" },
            "time_created": { "type": "string", "format": "date-time" },
            "events": {
                "type": "object",
                "properties": {
                    "new_process": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "cmdl": { "type": "string" },
                                "user": { "type": "string" }
                            },
                            "required": ["cmdl", "user"]
                        }
                    },
                    "network_connection": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "source_ip": { "type": "string" },
                                "destination_ip": { "type": "string" },
                                "destination_port": { 
                                    "type": "integer", 
                                    "minimum": 0, 
                                    "maximum": 65535 
                                }
                            },
                            "required": ["source_ip", "destination_ip", "destination_port"]
                        }
                    }
                },
                "required": ["new_process", "network_connection"]
            }
        },
        "required": ["submission_id", "device_id", "time_created", "events"]
    });

    // Parse the JSON string to a 'Value' type
    let json_data = match serde_json::from_str(json_str) {
        Ok(json_data) => json_data,
        Err(e) => {
            return Err(WorkerError::JsonDeserializationError(e.to_string()));
        },
    };

    // Validate the JSON data against the control schema
    match jsonschema::validate(&control_schema, &json_data) {
        Ok(_) => return Ok(()),
        Err(e) => {
            return Err(WorkerError::ValidationError(e.to_string()));
        }
    }
}

// Handles
// - Reading of submissions from the SQS
// - Validating and processing them
// - Calling write on contents
// - Either deleting the written submission from SQS, or freeing
//   the resource for other Processors upon failure
#[derive(Clone)]
struct SubmissionWorker {

    // SQS client for reading submissions
    sqs_client: SQSClient,

    // Kinesis client for writing events
    kinesis_client: aws_sdk_kinesis::Client,

    // Shard data store
    shard_data: Arc<RwLock<ShardDataStore>>,
}

impl SubmissionWorker {
    pub fn new(sqs_client: aws_sdk_sqs::Client, kinesis_client: aws_sdk_kinesis::Client, shard_data: Arc<RwLock<ShardDataStore>>) -> Self {
        Self { sqs_client, kinesis_client, shard_data }
    }

    // Main loop for submission processing
    pub async fn run(self) {

        // Polling loop with individual task spawn for each submission
        loop {
            // Read a submission from SQS, spawn a task to process it or log an error
            match self.sqs_client
                .receive_message()
                .send()
                .await {
                Ok(submission) => {
                    if let Some(messages) = submission.messages {
                        for message in messages {
                            let processor = self.clone();
                            let receipt_handle = match message.receipt_handle.clone() {
                                Some(receipt_handle) => receipt_handle,
                                None => {
                                    println!("No receipt handle found");
                                    continue;
                                }
                            };
                            task::spawn(async move {
                                let result = processor.process_submission(&message).await;

                                // If submission was valid but processing fails, release the message back to the queue
                                if result.is_err() && !matches!(result.clone().unwrap_err(), WorkerError::ValidationError(_)) {

                                    // Log the error
                                    println!("Error processing submission: {:?}", result.unwrap_err());

                                    // Set the visibility timeout to 0
                                    match processor.sqs_client
                                        .change_message_visibility()
                                        .receipt_handle(receipt_handle)
                                        .visibility_timeout(0)
                                        .send()
                                        .await
                                        {
                                            Ok(_) => {
                                                println!("Message released back to the queue");
                                            },
                                            Err(e) => {
                                                println!("Error changing visibility timeout: {:?}", e);
                                            }   
                                    };
                                }

                                // Invalid and written submissions are deleted from the SQS queue
                                else {
                                    // See if ValidationError, and log if yes
                                    if let Err(e) = result {
                                        println!("Validation failed: {:?}", e);
                                    }

                                    // Delete the message from the queue
                                    match processor.sqs_client
                                        .delete_message()
                                        .receipt_handle(receipt_handle)
                                        .send()
                                        .await
                                    {
                                        Ok(_) => {
                                            println!("Message deleted from the queue");
                                        },
                                        Err(e) => {
                                            println!("Error deleting message: {:?}", e);
                                        }
                                    };
                                }
                            });
                        }
                    }
                    // sleep for one second
                    sleep(time::Duration::from_secs(1)).await;
                },
                Err(e) => {
                    println!("Error receiving messages: {:?}", e);
                }
            };
        }
    }

    // Process submissions function
    pub async fn process_submission(&self, submission: &Message) -> Result<(), WorkerError> {
        if let Some(body) = &submission.body {
            // Decode the base64 encoded submission
            let decoded_data =  match decode_base64(body) {
                Ok(decoded_data) => decoded_data,
                Err(e) => {
                    return Err(WorkerError::ValidationError(e.to_string()));
                }
            };

            // Convert the decoded data to a string
            let submission_str = match String::from_utf8(decoded_data) {
                Ok(submission_str) => submission_str,
                Err(e) => {
                    return Err(WorkerError::ValidationError(e.to_string()));
                }
            };

            // Deserialize the json string to a Submission struct
            let submission: Submission = match serde_json::from_str(&submission_str) {
                Ok(submission) => submission,
                Err(e) => {
                    return Err(WorkerError::ValidationError(e.to_string()));
                }
            };

            // Validate the submission by checking if it matches the expected structure
            match is_valid_submission(&submission_str).await {
                Ok(_) => {},
                Err(e) => {
                    return Err(e);
                }
            };

            // Create a new vector for events in eventWrappers
            let mut events = Vec::<EventWrapper>::new();

            // Initiate order number for events
            let mut order = 0;

            // Loop through the new process events in the submission
            for event in submission.events.new_process {
                // Create an EventWrapper struct to hold the event data
                let event_wrapper = EventWrapper {
                    event_type: EventType::NewProcess(event.clone()),
                    event_id: "some_event_id".to_string(),
                    submission_id: submission.submission_id.clone(),
                    order: order as u32,
                    time_created: submission.time_created.clone(),
                };
                // Add the event to the events vector
                events.push(event_wrapper);
                order += 1;
            }

            // Loop through the network connection events in the submission
            for event in submission.events.network_connection {
                // Create an EventWrapper struct to hold the event data
                let event_wrapper = EventWrapper {
                    event_type: EventType::NetworkConnection(event.clone()),
                    event_id: "some_event_id".to_string(),
                    submission_id: submission.submission_id.clone(),
                    order: order as u32,
                    time_created: submission.time_created.clone(),
                };
                // Add the event to the events vector
                events.push(event_wrapper);
                order += 1;
            }

            // Start the writing task for current submission
            match &self.handle_writing(events).await {
                Ok(_) => return Ok(()),
                Err(e) =>
                    return Err(WorkerError::WritingError(vec![e.to_string()])),
            }
        } else {
            return Err(WorkerError::NoBodyError("No body found in submission".to_string()));
        }
    }

    async fn handle_writing(&self, events: Vec<EventWrapper>) -> Result<(), WorkerError> {
        // Create futures for all `write` calls
        let futures: Vec<_> = events
            .into_iter()
            .map(|event| self.write(event))
            .collect();

        // Await all futures concurrently
        let results = futures::future::join_all(futures).await;

        for result in results {
            if result.is_err() { // Check if write was successful
                return Err(result.unwrap_err());
            }
        }
        Ok(())
    }

    async fn write(&self, event: EventWrapper) -> Result<(), WorkerError> {

        // Set the initial retry wait time
        let mut wait_time = tokio::time::Duration::from_secs(1);

        // Set the maximum number of retries
        let max_retries = 3;

        // Serialize the event to a JSON string
        let json_string = match serde_json::to_string(&event) {
            Ok(s) => s,
            Err(e) => {
                println!("Error serializing event: {:?}", e);
                return Err(WorkerError::JsonSerializationError(e.to_string()));
            }
        };

        // PutRecord() needs input as Blobs, ceate a Blob from the JSON string
        let data = aws_sdk_kinesis::primitives::Blob::new(json_string.as_bytes());

        // Set the shard ID/Partition key
        let shard = "shardId-000000000000";

        // Initiate an array for possible errors
        let mut errors = Vec::<String>::new();

        // Try to write the event to the Kinesis stream
        for attempt in 0..max_retries {
            let result = self.kinesis_client
                .put_record()                                       // PutRecord operation
                .data(data.clone())                          // Clone of the original data as input
                .partition_key(shard)                        // Shard ID as partition key
                .stream_name("events")                       // Target Kinesis stream name
                .send()
                .await;

            match result {
                Ok(response) => {
                    println!("Put record successful: {:?}", response);
                    return Ok(()); // Success
                }

                // If not successful, log the error and retry
                Err(err) => {
                    // Add error to array
                    errors.push(err.to_string());
                    println!("Error writing event, attempt {}: {:?}", attempt + 1, err);
                    if attempt < max_retries - 1 {
                        sleep(wait_time).await;
                         // Exponential backoff to allow causes of possible congestion to ease off
                        wait_time *= 2;
                    }
                }
            }
        }

        // Failed after maximum number of retries
        return Err(WorkerError::WritingError(errors));
    
    }
}


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

    // Create a new, shared ShardDataStore instance using tokio::sync::RWLock
    let shard_data = Arc::new(RwLock::new(ShardDataStore::new()));


    // Create a new TrafficMonitor instance
    let mut monitor = TrafficMonitor::new(kinesis_client.clone(), time::Duration::from_secs(10), shard_data.clone());

    // Start monitoring traffic
    tokio::spawn(async move {
        monitor.start_monitoring().await;
    });

    // Create a new SubmissionWorker instance
    let worker = SubmissionWorker::new(sqs_client, kinesis_client, Arc::new(RwLock::new(ShardDataStore::new())));

    // Run the worker
    worker.run().await;
}