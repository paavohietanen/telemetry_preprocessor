
use aws_sdk_kinesis::{Client as KinesisClient, config as KinesisConfig, config::Region};
use aws_sdk_sqs::types::Message;
use aws_sdk_sqs::{Client as SQSClient, config as SQSConfig};
use base64::{self, Engine, engine::general_purpose};
use futures;
use jsonschema::{self};
use serde::{Serialize, Deserialize};
use tokio::time::sleep;
use tokio::{task, time};

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
async fn is_valid_submission(json_str: &str) -> bool {

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
        Err(error) => {
            println!("Error parsing JSON to 'Value': {:?}", error);
            return false;
        },
    };

    // Validate the JSON data against the control schema
    if jsonschema::is_valid(&control_schema, &json_data) {
        println!("Valid JSON!");
        return true;
    } else {
        println!("Invalid JSON!");
        return false;
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
}

impl SubmissionWorker {
    pub fn new(sqs_client: aws_sdk_sqs::Client, kinesis_client: aws_sdk_kinesis::Client) -> Self {
        Self { sqs_client, kinesis_client }
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
                            // TODO: error handling
                            task::spawn(async move {
                                processor.process_submission(&message).await;
                            });
                        }
                    }
                    // sleep for one second
                    sleep(time::Duration::from_secs(1)).await;
                },
                Err(error) => {
                    println!("Error receiving messages: {:?}", error);
                }
            };
        }
    }

    // Process submissions function
    pub async fn process_submission(&self, submission: &Message) {
        if let Some(body) = &submission.body {
            // Decode the base64 encoded submission
            let decoded_data =  match decode_base64(body) {
                Ok(decoded_data) => decoded_data,
                Err(error) => {
                    println!("Error decoding submission: {:?}", error);
                    return;
                }
            };

            // Convert the decoded data to a string
            let submission_str = match String::from_utf8(decoded_data) {
                Ok(submission_str) => submission_str,
                Err(error) => {
                    println!("Error converting submission to string: {:?}", error);
                    return;
                }
            };

            // Deserialize the json string to a Submission struct
            let submission: Submission = match serde_json::from_str(&submission_str) {
                Ok(submission) => submission,
                Err(error) => {
                    println!("Error deserializing submission: {:?}", error);
                    return;
                }
            };

            // Validate the submission by checking if it matches the expected structure
            match is_valid_submission(&submission_str).await {
                true => println!("Submission is valid!"),
                false => {
                    println!("Submission is invalid!");
                    return;
                },
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
                Ok(_) => println!("All events written successfully."),
                Err(e) => eprintln!("Failed to write events: {:?}", e),
            }

        }
    }

    async fn handle_writing(&self, events: Vec<EventWrapper>) -> Result<(), Box<dyn std::error::Error>> {
        // Create futures for all `write` calls
        let futures: Vec<_> = events
            .into_iter()
            .map(|event| self.write(event))
            .collect();

        // Await all futures concurrently
        let results = futures::future::join_all(futures).await;

        for result in results {
            if !result { // Check if write was successful
                return Err("Failed to write event".into());
            }
        }
        Ok(())
    }

    async fn write(&self, event: EventWrapper) -> bool {

        // Set the initial retry wait time
        let mut wait_time = tokio::time::Duration::from_secs(1);

        // Set the maximum number of retries
        let max_retries = 3;

        // Serialize the event to a JSON string
        let json_string = match serde_json::to_string(&event) {
            Ok(s) => s,
            Err(err) => {
                println!("Error serializing event: {:?}", err);
                return false;
            }
        };

        // PutRecord() needs input as Blobs, ceate a Blob from the JSON string
        let data = aws_sdk_kinesis::primitives::Blob::new(json_string.as_bytes());

        // Set the shard ID/Partition key
        let shard = "shardId-000000000000";

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
                    return true; // Success
                }

                // If not successful, log the error and retry
                Err(err) => {
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
        false
    
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

    // Create a new SubmissionWorker instance
    let worker = SubmissionWorker::new(sqs_client, kinesis_client);

    // Run the worker
    worker.run().await;
}