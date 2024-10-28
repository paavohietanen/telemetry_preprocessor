use aws_config::imds::client;
use aws_config::BehaviorVersion;
use aws_sdk_kinesis::config;
use aws_sdk_kinesis::error::SdkError;
use aws_sdk_kinesis::operation::put_record::PutRecordError;
use aws_sdk_kinesis::types::error;
use aws_sdk_sqs::types::Message;
use aws_sdk_sqs::{Client, Config, config::Region};
use base64::{self, Engine, engine::general_purpose};
use futures;
use jsonschema::{self, ValidationError};
use serde::{Serialize, Deserialize};
use tokio::time::sleep;
use std::sync::Arc;
use tokio::{runtime::Runtime, sync::Semaphore};
use tokio::{task, time};
use tokio_stream::StreamExt;

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
    NewProcess(NewProcess),
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
    println!(" === INITIATING SCHEMA === ");
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

    let json_data = match serde_json::from_str(json_str) {
        Ok(json_data) => json_data,
        Err(error) => {
            println!("Error parsing JSON to 'Value': {:?}", error);
            return false;
        },
    };

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
    sqs_client: aws_sdk_sqs::Client,
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
            let result = match self
                .sqs_client
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

            // Loop through the new process events in the submission
            for (order, event) in submission.events.new_process.iter().enumerate() {
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
            }

            // Loop through the network connection events in the submission
            for (order, event) in submission.events.network_connection.iter().enumerate() {
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
            }

            /*match &self.handle_writing(events).await {
                Ok(_) => println!("All events written successfully."),
                Err(e) => eprintln!("Failed to write events: {:?}", e),
            }*/

        }
    }

}


#[tokio::main]
async fn main() {
    // Build the configuration for SQS
    let sqs_config = aws_sdk_sqs::config::Builder::new()
        .region(Region::new("eu-west-1"))
        .endpoint_url("http://localhost:4566/000000000000/submissions") // Queue URL: Differs from 'list-queues' URL, that one gives error
        .credentials_provider(aws_sdk_sqs::config::Credentials::new(
            "some_key_id",  // Access key for LocalStack
            "some_secret",  // Secret key for LocalStack
            None,               // Optional session token
            None,               // Expiry (optional)
            "localstack",       // Provider name
        ))
        .build();

    // Construct the SQS client based on our configuration
    let sqs_client = aws_sdk_sqs::Client::from_conf(sqs_config);

    // Build the configuration for Kinesis
    let kinesis_config = aws_sdk_kinesis::config::Builder::new()
        .region(Region::new("eu-west-1"))
        .endpoint_url("http://localhost:4566/") // Kinesis uses the root URL
        .credentials_provider(aws_sdk_kinesis::config::Credentials::new(
            "some_key_id",  // Access key for LocalStack
            "some_secret",  // Secret key for LocalStack
            None,               // Optional session token
            None,               // Expiry (optional)
            "localstack",       // Provider name
        ))
        .build();

    // Construct the Kinesis client based on our configuration
    let kinesis_client = aws_sdk_kinesis::Client::from_conf(kinesis_config);

    // Create a new SubmissionWorker instance
    let worker = SubmissionWorker::new(sqs_client, kinesis_client);

    // Run the worker
    worker.run().await;
}