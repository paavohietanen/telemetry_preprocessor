use aws_config::imds::client;
use aws_config::BehaviorVersion;
use aws_sdk_kinesis::config;
use aws_sdk_sqs::types::Message;
use aws_sdk_sqs::{Client, Config, config::Region};
use std::sync::Arc;
use tokio::{runtime::Runtime, sync::Semaphore};
use tokio::{task, time};
use tokio_stream::StreamExt;


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

    // Process submissions function
    pub async fn process_submission(&self, submission: &Message) {
        // Code for reading and processing from SQS here
        // Read the message contents and log them
        if let Some(body) = &submission.body {
            println!("Received message: {}", body);
        }
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
                            task::spawn(async move {
                                processor.process_submission(&message).await;
                            });
                        }
                    }
                    time::sleep(time::Duration::from_secs(1)).await;
                },
                Err(error) => {
                    println!("Error receiving messages: {:?}", error);
                }
            };
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