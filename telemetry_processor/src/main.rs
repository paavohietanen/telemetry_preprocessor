use aws_sdk_sqs::{Client, Config, config::Region};
use std::sync::Arc;
use tokio::{runtime::Runtime, sync::Semaphore};
use tokio::task;
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
    queue_url: String,
}

impl SubmissionWorker {
    pub fn new(client: Client, queue_url: String) -> Self {
        Self { client, queue_url }
    }

    // Process submissions function
    pub async fn process_submission(&self) {
        // Code for reading and processing from SQS here
    }

    // Main loop for submission processing
    pub async fn run(self) {
        loop {
            // Polling loop with individual task spawn for each submission
            if let Ok(msg) = self.client.receive_message().queue_url(&self.queue_url).send().await {
                if let Some(messages) = msg.messages {
                    for message in messages {
                        let processor = self.clone();
                        task::spawn(async move {
                            processor.process_submission().await;
                        });
                    }
                }
            }
        }
    }
}


#[tokio::main]
async fn main() {
    let region = Region::new("us-west-2"); // Adjust as necessary
    let config = aws_config::load_from_env().await;
    let client = Client::new(&config);
    let queue_url = "http://localstack:4566".to_string();
    let processor = SubmissionWorker::new(client, queue_url);

    processor.run().await; // Start the processing loop
}