use aws_config::imds::client;
use aws_config::BehaviorVersion;
use aws_sdk_kinesis::config;
use aws_sdk_sqs::types::Message;
use aws_sdk_sqs::{Client, Config, config::Region};
use base64::{self, Engine, engine::general_purpose};
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

// Helper function to decode base64 binary data
fn decode_base64(encoded_data: &str) -> Result<Vec<u8>, base64::DecodeError> {
    general_purpose::STANDARD.decode(encoded_data)
}

// Helper function to validate JSON against a schema
async fn validate_json(json_str: &str) -> bool {
    // Initiate the control schema
    print!(" === INITIATING SCHEMA === ");
    let control_schema = serde_json::json!({
        "$schema": "http://json-schema.org/draft-2020-12/schema",
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

    // Initiate schema validator
    let validator = match jsonschema::validator_for(&control_schema) {
        Ok(validator) => validator,
        Err(error) => {
            println!("Error creating JSON schema validator: {:?}", error);
            return false;
        },
    };
    println!(" === VALIDATING JSON === ");
    let json_data = match serde_json::from_str(json_str) {
        Ok(json_data) => json_data,
        Err(error) => {
            println!("Error parsing JSON to 'Value': {:?}", error);
            return false;
        },
    };
    match validator.validate(&json_data) {
        Ok(_) => true,
        Err(error) => {
            println!("Validation error: {:?}", error);
            false
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
    client: Client,
}

impl SubmissionWorker {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    // Main loop for submission processing
    pub async fn run(self) {

        // Polling loop with individual task spawn for each submission
        loop {
            // Read a submission from SQS, spawn a task to process it or log an error
            let result = match self
                .client
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
                    //sleep(time::Duration::from_secs(1)).await;
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
            //TODO: Json schema validation
            // Validate the submission by checking if it matches the expected structure
            //let is_valid = validate_json(&submission_str).then({print!("Submission is valid: {:?}", is_valid);});
            

        }
    }

    
}


#[tokio::main]
async fn main() {
    // Build the configuration that we want to use
    let config = aws_sdk_sqs::config::Builder::new()
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

    // Construct the client based on our configuration
    let client = aws_sdk_sqs::Client::from_conf(config);

    // Create a new SubmissionWorker instance
    let worker = SubmissionWorker::new(client);

    // Run the worker
    worker.run().await;
}