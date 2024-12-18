use aws_sdk_kinesis::{
    error::SdkError,
    operation::put_record::PutRecordError
};
use aws_sdk_sqs::client::Client as SQSClient;
use aws_sdk_sqs::types::Message;
use crate::{config,
        modules::{
            error::WorkerError,
            shard_data_store::ShardDataStore,
            submission_worker::{
                submission::Submission,
                event::
                {
                    EventWrapper,
                    EventType
                },
                helpers::{
                    decode_base64,
                    is_valid_submission,
                },
            }
        },
};
use std::sync::Arc;
use serde::{Serialize, Deserialize};
use tokio::{
    sync::RwLock,
    task,
    time::{
        Duration,
        sleep,
    },
};
use uuid::Uuid;

mod helpers;
mod submission;
mod event;

// Handles
// - Reading of submissions from the SQS
// - Validating and processing them
// - Calling write on contents
// - Either deleting the written submission from SQS, or freeing
//   the resource for other Processors upon failure
#[derive(Clone)]
pub struct SubmissionWorker {

    // SQS client for reading submissions
    sqs_client: SQSClient,

    // Kinesis client for writing events
    kinesis_client: aws_sdk_kinesis::Client,

    // ShardDataStore for recording metrics
    // A shared resource between TrafficMonitor and SubmissionWorkers
    shard_data: Arc<RwLock<ShardDataStore>>,

    // Config for SubmissionWorker
    config: config::SubmissionWorkerConfig,
}

impl SubmissionWorker {
    pub fn new(sqs_client: aws_sdk_sqs::Client, kinesis_client: aws_sdk_kinesis::Client, shard_data: Arc<RwLock<ShardDataStore>>, config: config::SubmissionWorkerConfig) -> Self {
        Self { sqs_client, kinesis_client, shard_data, config }
    }

    // Main loop for submission processing
    pub async fn run(self) {

        // Polling loop with individual task spawn for each submission
        loop {
            // Read a submission from SQS, spawn a task to process it or log an error
            match self.sqs_client
                .receive_message()
                .visibility_timeout(self.config.visibility_timeout)
                .max_number_of_messages(self.config.max_number_of_messages)
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
                                        // todo: log these to a file instead
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
                    sleep(Duration::from_secs(self.config.polling_interval)).await;
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
                    event_id: Uuid::new_v4().to_string(),
                    submission_id: submission.submission_id.clone(),
                    order: order as u32,
                    time_created: submission.time_created.clone(),
                    time_processed: chrono::Local::now().to_rfc3339(),
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
                    event_id: Uuid::new_v4().to_string(),
                    submission_id: submission.submission_id.clone(),
                    order: order as u32,
                    time_created: submission.time_created.clone(),
                    time_processed: chrono::Local::now().to_rfc3339(),
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
        // Clone the id of this submission to a new variable
        let submission_id = &events[0].submission_id.clone();
        println!(" - Initiating submission writing for {:?}...", submission_id);

        // Initiate a variable for shard id
        let shard_id: String;
        { // Acquire write lock on `shard_data`
            let mut shard_data = self.shard_data.write().await;

            // Get the healthiest shard to write to
            shard_id = match shard_data.get_healthiest_shard() {
                Ok(id) => id,
                Err(e) => return Err(e),
            };

            // Add this submission to the shard_data to note that it is being written on it
            shard_data.add_submission(&shard_id, &submission_id);
        } // Release the write lock

        // Create futures for all `write` calls
        let futures: Vec<_> = events
            .into_iter()
            .map(|event| self.write(event, shard_id.clone()))
            .collect();

        // Await all futures concurrently
        let results = futures::future::join_all(futures).await;

        { // Acquire write lock on `shard_data` again when results have arrived
            let mut shard_data = self.shard_data.write().await;

            // Delete the submission from the shard_data
            shard_data.remove_submission(&shard_id, &submission_id);
        } // Release the write lock

        for result in results {
            if result.is_err() { // Check if write was successful
                return Err(result.unwrap_err());
            }
        }
        Ok(())
    }

    async fn write(&self, event: EventWrapper, mut shard_id: String) -> Result<(), WorkerError> {
        println!(" -- Writing event to shard {}", shard_id);
        // Set the initial retry wait time
        let mut wait_time = Duration::from_secs(self.config.retry_interval);

        // Set the maximum number of retries
        let max_retries = self.config.max_retry_attempts;

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

        // Initiate an array for possible errors
        let mut errors = Vec::<String>::new();

        // Try to write the event to the Kinesis stream
        for attempt in 0..max_retries {

            // Check that the shard we would use exists
            shard_id = match self.check_and_update_shard(shard_id, &event.submission_id).await {
                Ok(id) => id,
                Err(e) => return Err(e),
            };

            let result = self.kinesis_client
                .put_record()                                       // PutRecord operation
                .data(data.clone())                          // Clone of the original data as input
                .partition_key(shard_id.clone())                        // Shard ID as partition key
                .stream_name("events")                       // Target Kinesis stream name
                .send()
                .await;

            match result {
                Ok(response) => {
                    // Record metrics for the traffic monitor
                    { // Acquire a write lock on `shard_data`
                        let mut shard_data = self.shard_data.write().await;

                        // Record the amount of bytes written to the shard
                        shard_data.record_metrics(shard_id, Some(json_string.as_bytes().len() as u64), Some(1), None).await;
                    }

                    println!(" --- Event written to shard {}", response.shard_id);

                    // Return Ok
                    return Ok(()); // Success
                }

                // If not successful, log the error and retry
                Err(err) => {
                    // Add error to array
                    errors.push(err.to_string());
                    println!(" --- Error writing event, attempt {}: {:?}", attempt + 1, err);

                    // Match against the error type and react accordingly
                    match err {
                        SdkError::ServiceError(service_err) => {
                            // We need to extract the PutRecordError from the ServiceError
                            let put_record_error = service_err.err(); // The service_err itself is the error we need
                            
                            // Match against the PutRecordError variants
                            match put_record_error {
                                PutRecordError::ProvisionedThroughputExceededException(e) => {

                                    // Rate has exceeded for this shard
                                    // Record it in the shard data store
                                    { // Acquire a write lock
                                        let mut shard_data = self.shard_data.write().await;
                                        shard_data.record_metrics(shard_id, None, None, Some(1)).await;
                                    } // Release the write lock

                                    // Get a new shard for the next attempt
                                    { // Acquire a read lock
                                        let shard_data = self.shard_data.read().await;
                                        // Get a new shard
                                        shard_id = match shard_data.get_healthiest_shard() {
                                            Ok(id) => id.clone(), // Clone the id to return it
                                            Err(e) => return Err(e.clone()),
                                        };
                                    } // Release the read lock
                                    println!(" ---- Retrying with shard {}", shard_id);
                                }
                                // Handle other specific PutRecordError cases if needed
                                // Todo: Implement logic
                                _ => {
                                    // Log or handle other types of errors
                                    println!("Other PutRecordError occurred: {:?}", put_record_error);
                                }
                            }
                        }
                        SdkError::TimeoutError(_) => {
                            // Handle timeout errors
                            // Todo: Implement logic
                            println!("Request timed out.");
                        }
                        _ => {
                            // Handle other generic errors
                            // Todo: Implement logic
                            println!("An unexpected error occurred: {:?}", err);
                        }
                    }

                    // Wait for the retry time
                    tokio::time::sleep(wait_time).await;
                    
                    // Double the wait time for the next retry
                    // In order to let possible congestion clear
                    wait_time *= 2;
                }
            }
        }

        // Failed after maximum number of retries
        return Err(WorkerError::WritingError(errors));
    
    }

    // Function to check if the shard exists, and returning a new one if it doesn't
    async fn check_and_update_shard(&self, mut shard_id: String, submission_id: &str, ) -> Result<String, WorkerError> {
        { // Acquire read lock
            let shard_data = self.shard_data.read().await; 

            // Check if the shard is active
            if !shard_data.shard_is_active(shard_id.clone()) {

                // If shard doesn't exist anymore, get a new shard
                let new_shard_id = match shard_data.get_healthiest_shard() {
                    Ok(id) => id.clone(), // Clone the id to return it
                    Err(e) => return Err(e.clone()),
                };
                shard_id = new_shard_id; // Assign the new shard ID
            }
        } // Lock is released here

        { // Acquire write lock to add this submission to the new shard
            let mut shard_data = self.shard_data.write().await;
            shard_data.add_submission(&shard_id, submission_id);
        } // Release the write lock

        // Return the shard ID
        Ok(shard_id)
    }
}
