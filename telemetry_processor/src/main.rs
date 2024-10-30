use aws_sdk_kinesis::{
    Client as KinesisClient, 
    config as KinesisConfig,
    config::Region,
    error::SdkError,
    types::{Shard, ShardIteratorType},
    operation::put_record::PutRecordError
};
use aws_sdk_sqs::types::Message;
use aws_sdk_sqs::{Client as SQSClient, config as SQSConfig};
use base64::{self, Engine, engine::general_purpose};
use futures;
use jsonschema;
use serde::{Serialize, Deserialize};
use std::{f32::consts::E, fmt};
use tokio::time::sleep;
use tokio::{task, time, sync::RwLock};
use std::{
    sync::Arc,
    time::SystemTime
};
use num_bigint::{BigInt, BigUint};
use num_traits::Zero;

const MAX_CAPACITY: f64 = 1_000_000.0; // 1 MB/s capacity of a shard
const SPLIT_USAGE_THRESHOLD: f64 = 0.8;
const SPLIT_TP_ERROR_THRESHOLD: u64 = 2;
const MERGE_USAGE_THRESHOLD: f64 = 0.3;
const MERGE_TP_ERROR_THRESHOLD: u64 = 0;
const MONITORING_DURATION: u64 = 10; // Last 10 seconds for monitoring
const ENTRY_LIFETIME: std::time::Duration = std::time::Duration::from_secs(60); // 60 seconds

// ShardMetrics struct to hold metrics for each shard
#[derive(Debug, Clone)]
struct ShardMetrics {
    // Identifier of the shard
    shard_id: String,
    // Starting hash key of the shard
    // Needed in shard splitting and merging
    starting_hash_key: String,
    // Ending hash key of the shard
    // Needed in shard merging
    ending_hash_key: String,
    // Average data processed per entry over TrafficMonitor's
    // observation duration
    average_data_processed: f64,
    // Number of ProvisionedThrougputErrors ovre TrafficMonitor's
    // observation duration
    recent_throughput_errors: u64,
    // Traffic data entries for this shard
    entries: Vec<ShardMetricEntry>,
    // Submissions that are being written to this shard
    submissions: Vec<String>,
    // Whether the shard is active or not
    is_active: bool,
}

// A single entry of shard data
#[derive(Debug, Clone)]
struct ShardMetricEntry {
    timestamp: SystemTime,
    data_processed: Option<u64>,
    records_read: Option<u64>,
    throughput_errors: Option<u64>,
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

    // Add a single new shard with ShardMetrics object
    pub fn add_shard(&mut self, shard: ShardMetrics) {
        println!(" ~~~ Shard {} added", shard.shard_id);
        self.shards.push(shard);
    }

    // Check if shard is active
    pub fn shard_is_active(&self, shard_id: String) -> bool {
        self.shards.iter().find(|s| s.shard_id == shard_id).map(|s| s.is_active).unwrap_or(false)
    }

    // Get shard by id
    pub fn get_shard_by_id(&self, shard_id: String) -> Option<ShardMetrics> {
        self.shards.iter().find(|s| s.shard_id == shard_id).map(|s| s.clone())
    }

    // Get shard for modification
    pub fn get_shard_by_id_mut(&mut self, shard_id: String) -> Option<&mut ShardMetrics> {
        self.shards.iter_mut().find(|s| s.shard_id == shard_id)
    }

    // Get shard
    pub fn get_healthiest_shard(&self) -> Result<String, WorkerError> {
        // Make a new list and fill it with active shards with least amount of submissions
        let mut shards_with_least_submissions = Vec::new();
        let mut min_submissions = std::usize::MAX;
        //Keep only shards that have is_active == true
        for shard in &self.shards {
            if shard.is_active {
                if shard.submissions.len() < min_submissions {
                    min_submissions = shard.submissions.len();
                    shards_with_least_submissions.clear();
                    shards_with_least_submissions.push(shard);
                } else if shard.submissions.len() == min_submissions {
                    shards_with_least_submissions.push(shard);
                }
            }
        }

        // Iterate through the list and get the shard with lowest traffic
        let mut min_traffic = std::f64::MAX;
        let mut shard_with_lowest_traffic = None;
        for shard in shards_with_least_submissions {
            if shard.average_data_processed < min_traffic {
                min_traffic = shard.average_data_processed;
                shard_with_lowest_traffic = Some(shard);
            }
        }

        // Return the shard
        match shard_with_lowest_traffic {
            Some(shard) => Ok(shard.shard_id.clone()),
            // todo: TrafficError
            None => Err(WorkerError::ValidationError("No shard found".to_string())),
        }
    }

    // Add a submission id to the shard
    pub fn add_submission(&mut self, shard_id: &str, submission_id: &str) {
        // Find the shard in the shards vector
        let shard = self.shards.iter_mut().find(|s| s.shard_id == shard_id);

        // If the shard is found, add the submission id
        if let Some(shard) = shard {
            shard.submissions.push(submission_id.to_string());
        }
    }

    // Remove a submission id from the shard
    pub fn remove_submission(&mut self, shard_id: &str, submission_id: &str) {
        // Find the shard in the shards vector
        let shard = self.shards.iter_mut().find(|s| s.shard_id == shard_id);

        // If the shard is found, remove the submission id
        if let Some(shard) = shard {
            shard.submissions.retain(|s| s != submission_id);
            println!("Submission {} removed from shard {}", submission_id, shard_id);
        }
    }

    // Function to record metrics data for the traffic monitor
    // Currently only stores written bytes and records
    pub async fn record_metrics(&mut self, shard_id: String, data_size: Option<u64>, record_count: Option<u64>, throughput_errors: Option<u64>) {
        // Find the shard in the shards vector
        let shard = self.shards.iter_mut().find(|s| s.shard_id == shard_id);

        // If the shard is found, update the metrics
        if let Some(shard) = shard {
            shard.entries.push(ShardMetricEntry {
                timestamp: SystemTime::now(),
                data_processed: data_size,
                records_read: record_count,
                throughput_errors: throughput_errors,
            });
        } else {
            // If the shard is not found, create a new entry, print an error
            println!("Shard {} not found", shard_id);
        }        
    }
           
}

struct TrafficMonitor {
    // Kinesis client to fetch shards
    kinesis_client: aws_sdk_kinesis::Client,
    // Interval for the monitoring loop
    check_interval: tokio::time::Duration,
    // ShardDataStore for managing traffic data
    // A shared resource between TrafficMonitor and SubmissionWorkers
    shard_data: Arc<RwLock<ShardDataStore>>,
}

impl TrafficMonitor {

    pub fn new(kinesis_client: aws_sdk_kinesis::Client, check_interval: tokio::time::Duration, shard_data: Arc<RwLock<ShardDataStore>>) -> Self {
        Self {
            kinesis_client,
            check_interval,
            shard_data,
        }
    }

    // Function to start monitoring traffic
    pub async fn start_monitoring(&mut self) {

        // First update the shard data of the shard data store
        match self.update_shard_data_store_list().await {
            Ok(_) => println!(" Shard data store updated"),
            // TODO: Check if requires better handling
            Err(e) => println!("Error updating shard data store: {:?}", e),
        };

        // Start the monitoring loop
        loop {
            // Check traffic conditions
            self.manage_traffic().await;
            tokio::time::sleep(self.check_interval).await;
        };
    }

    // Update shard list of the shard data store to reflect the current state of the stream
    async fn update_shard_data_store_list(&self) -> Result<(), WorkerError> {

        println!(" ~ Updating shard data store list...");

        // Kinesis might return the shards in multiple responses
        // To get all subsets of shards, we need a marker from where to start
        // retrieving the next batch of shards, if any left.
        // Initialize this first to None, to start from the beginning
        let mut next_token: Option<String> = None;

        // Loop to handle paginated results
        loop {

            // Include a request to list the shards of the stream
            let mut request = self.kinesis_client.list_shards()
            .stream_name("events"); // Stream name

            // Only add exclusive_start_shard_id to the request if next_token is Some
            if let Some(ref token) = next_token {
                request = request.next_token(token); // Use token directly
            }

            // Send the request and await the response
            let response = request.send().await;

            match response {
                Ok(ref res) => {
                    println!(" ~~ Handling shards in response...");
                    if let Some(ref shards) = res.shards {
                        // Append the shard IDs to the list
                        for shard in shards {

                            // Check if the shard is active (can be written to) or inactive (can't be written to).
                            // Active shards are the ones with an open-ended sequence number range, having no ending sequence number.
                            let is_active = shard.sequence_number_range().and_then(|range| range.ending_sequence_number()).is_none();

                            // See if the shard exists in the shard data store
                            let existing_shard_opt: Option<ShardMetrics>;
                            
                            { // Acquire read lock
                                let shard_data = self.shard_data.read().await;
                                existing_shard_opt = shard_data.get_shard_by_id(shard.shard_id.clone());
                            } // Release the read lock

                            if existing_shard_opt.is_some() {
                                let existing_shard = existing_shard_opt.unwrap();

                                // If yes, check if the activity is up to date
                                let activity_up_to_date = existing_shard.is_active == is_active;
                                
                                // If yes, continue. If not, update activity
                                match activity_up_to_date {
                                    true => continue,
                                    false => {
                                        { // Acquire write lock to update the shard
                                            let mut shard_data = self.shard_data.write().await;
                                            
                                            // Get the shard from the shard data store
                                            let inactive_shard = shard_data.get_shard_by_id_mut(shard.shard_id.clone()).unwrap();

                                            // Set the shard to inactive
                                            inactive_shard.is_active = is_active;
                                            println!(" ~~~ Shard {} activity status updated to {:?}", shard.shard_id, is_active);
                                        } // Release the write lock
                                    }
                                }
                            // If the shard does not exist, add it
                            } else {
                                { // Acquire write lock to add the shard
                                    let mut shard_data = self.shard_data.write().await;
                                    shard_data.add_shard(ShardMetrics {
                                        shard_id: shard.shard_id.clone(),
                                        starting_hash_key: match &shard.hash_key_range {
                                            Some(range) =>  range.starting_hash_key().to_string(),
                                            None => return Err(WorkerError::ValidationError("Hash key range not found".to_string())),
                                        },
                                        ending_hash_key: match &shard.hash_key_range {
                                            Some(range) =>  range.ending_hash_key().to_string(),
                                            None => return Err(WorkerError::ValidationError("Hash key range not found".to_string())),
                                        },
                                        average_data_processed: 0.0,
                                        recent_throughput_errors: 0,
                                        entries: Vec::new(),
                                        submissions: Vec::new(),
                                        is_active,
                                    });
                                } // Release the write lock
                            }
                        }
                        
                        // Get the next token
                        next_token = res.next_token.clone();

                        // Break the loop if there are no more shards to process
                        if next_token.is_none() {
                            return Ok(());
                        }
                    }
                }
                Err(err) => {
                    println!("Error fetching shards: {:?}", err);
                    break; // Handle the error as appropriate for your application
                }
            }
        }
        println!(" ~~~~~ Shard data store update concludes");
        Ok(())
    }

    
    // Function to check traffic and manage shards
    pub async fn manage_traffic(&mut self) {
        println!(" # Checking traffic...");

        // Get the current time
        let current_time = SystemTime::now();

        // Initiate a new list for shards to split
        let mut shards_to_split = Vec::new();

        // Initiate a new list for shards to merge
        let mut shards_to_merge = Vec::new();

        // Lock writing for the whole loop, so that SubmissionWorkers
        // don't change the data while it's calculated
        { // Acquire a write lock on `shard_data`
            let shard_data = self.shard_data.write().await;

            // Iterate through shards
            for mut metrics in shard_data.shards.clone() {
                println!(" ## Checking shard {}", metrics.shard_id);
                println!(" ## Shard activity status: {}", metrics.is_active);

                // Remove entries older than ENTRY_LIFETIME
                metrics.entries.retain(|entry| {
                    current_time.duration_since(entry.timestamp).map_or(false, |elapsed| {
                        elapsed <= ENTRY_LIFETIME
                    })
                });

                // If shard is inactive, skip the rest of the loop
                if !metrics.is_active {
                    continue;
                }
                // Calculate the average data processed per second and the number of throughput errors
                // from all entries that fall to the monitoring duration
                let mut total_data_processed: f64 = 0.0;
                let mut number_of_entries: u64 = 0;
                let mut throughput_errors: u64 = 0;
                // Iterate through shard metrics
                for entry in &metrics.entries {
                    // Check if the metrics were recorded in the last 10 seconds
                    if let Ok(elapsed) = current_time.duration_since(entry.timestamp) {

                        if elapsed.as_secs() <= MONITORING_DURATION {
                            if entry.data_processed.is_some() {
                                // Add the data processed to the total
                                total_data_processed += entry.data_processed.unwrap() as f64;
                                number_of_entries += 1;
                            }
                            if entry.throughput_errors.is_some() {
                                // Add the throughput errors to the total
                                throughput_errors += entry.throughput_errors.unwrap();
                            }
                        }
                    }
                }

                // Calculate the average data processed
                // and assign it to the shard metrics object
                metrics.average_data_processed = total_data_processed / number_of_entries as f64;

                // Calculate the percentage of capacity used in average
                let usage_percentage = (metrics.average_data_processed as f64) / MAX_CAPACITY * 100.0;

                // Assign the throughput errors to the shard metrics object
                metrics.recent_throughput_errors = throughput_errors;

                // If the usage percentage or the amount of recent throughput errors exceed their thresholds,
                // assign to be split
                if (usage_percentage >= SPLIT_USAGE_THRESHOLD * 100.0) || (metrics.recent_throughput_errors >= SPLIT_TP_ERROR_THRESHOLD) {
                    println!(" ## Shard {} is over 80% capacity, assigning for splitting...", metrics.shard_id);
                    shards_to_split.push(metrics.shard_id.clone());
                }

                // If the usage percentage is below or equal to the merge threshold, and there have been no
                // recent throughput errors, assign as a merge candidate
                if (usage_percentage <= MERGE_USAGE_THRESHOLD * 100.0) && (metrics.recent_throughput_errors <= MERGE_TP_ERROR_THRESHOLD) {
                    println!(" ## Shard {} is below or equal to 30% capacity, candidate for merging...", metrics.shard_id);
                    shards_to_merge.push(metrics.shard_id.clone());
                }
                
                println!(" ### Shard {} average data processed: {}", metrics.shard_id, usage_percentage);
                println!(" ### Shard {} throughput errors over observation period: {}", metrics.shard_id, metrics.recent_throughput_errors);
                println!(" ### Submissions being written to shard {}: {:?}", metrics.shard_id, metrics.submissions);
            }
        } // Release the write lock

        // Go through the shards to split, and split them
        for shard_id in shards_to_split {
            self.split_shard(&shard_id).await;
        }

        // If there are two or more mergeable shards, merge them
        if shards_to_merge.len() >= 2 {
            self.handle_merging(shards_to_merge).await;
        }
    }

    async fn split_shard(&mut self, shard_id: &String) {
        // Use the Kinesis client to split the shard
        println!(" = Splitting shard: {:?}", shard_id);

        // Initiate variable for the new starting hash key
        let new_starting_hash_key: String;
        {// Acquire a read lock on `shard_data`
            let shard_data = self.shard_data.read().await;
            // Get the shard
            let shard = match shard_data.get_shard_by_id(shard_id.clone()) {
                Some(s) => s,
                None => {
                    println!("Shard {} not found", shard_id);
                    return;
                },
            };
            // Calculate the new starting hash key
            new_starting_hash_key = match Self::calculate_new_starting_hash_key(&shard.starting_hash_key, &shard.ending_hash_key) {
                Ok(key) => key,
                Err(e) => {
                    println!(" == Error calculating new starting hash key: {:?}", e);
                    return;
                },
            };
        }
    
        let split_result = self.kinesis_client
            .split_shard()
            .stream_name("events")
            .shard_to_split(shard_id.clone())
            .new_starting_hash_key(new_starting_hash_key) 
            .send()
            .await;
    
        match split_result {
            Ok(_) => println!(" === Successfully split shard: {:?}", shard_id),
            Err(err) => println!(" === Error splitting shard: {:?}", err),
        }

        // Update the shard data store list
        match self.update_shard_data_store_list().await {
            Ok(_) => println!(" ==== Shard data store updated"),
            Err(e) => println!(" ==== Error updating shard data store: {:?}", e),
        }
    }

    fn calculate_new_starting_hash_key(original_starting_hash_key: &str, original_ending_hash_key: &str) -> Result<String, WorkerError> {
        // Convert the hash keys from strings to u64 (or any other numeric type as needed)
        let starting_key: BigInt = match original_starting_hash_key.parse::<BigInt>() {
            Ok(key) => key,
            Err(_) => {print!("ERROR PARSING STARTING KEY");
                return Err(WorkerError::ValidationError("Error parsing starting key".to_string()));
            }
            ,
        };
        println!("Starting key: {:?}", starting_key);
        println!("Ending key: {:?}", original_ending_hash_key);
        let ending_key: BigInt = match original_ending_hash_key.parse::<BigInt>() {
            Ok(key) => key,
            Err(_) => {print!("ERROR PARSING ENDING KEY");
                return Err(WorkerError::ValidationError("Error parsing ending key".to_string()))}
        };
    
        // Calculate the midpoint (this is the new starting hash key for one of the split shards)
        let new_starting_key: BigInt = (starting_key + ending_key) / 2;
    
        // Convert back to string
        Ok(new_starting_key.to_string())
    }
    
    // Function to merge a list of shards in pairs
    pub async fn handle_merging(&mut self, mergable_ids: Vec<String>) -> Result<(), WorkerError> {

        // Initialize a vector for objects that were merged
        let mut merged_shards: Vec<String> = Vec::new();
        println!("%%%%%%%%%%%%%%%%%%%%%%%%% Merging shards...");
        // Initialize a vector for corresponding SharedMetrics objects
        let mut shards: Vec<ShardMetrics> = Vec::new();
        { // Acquire a read lock
            let shard_data = self.shard_data.read().await;
            // Get the ShardMetrics objects for the shards
            for shard_id in &mergable_ids {
                if let Some(shard) = shard_data.get_shard_by_id(shard_id.clone()) {
                    shards.push(shard);
                }
            }
        } // Release the read lock

        // Iterate through the shards
        for i in 0..shards.len() {
            // Check if the shard can be merged
            if shards[i].is_active {
                // Check for a pair to merge with
                for j in (i + 1)..shards.len() {
                    println!(" %%%%% ACTIVITY SHARD i {:?} {:?} SHARD j {:?} {:?}", shards[i].shard_id, shards[i].is_active, shards[j].shard_id, shards[j].is_active);
                    if shards[j].is_active && Self::is_adjacent(&shards[i], &shards[j]) {
                        println!("%%%%%%%%%%%%%%%%%%%%%%%%% Merging shards: {:?} {:?}", shards[i].shard_id, shards[j].shard_id);
                        let merge_result = &self.merge_shard(&shards[i].shard_id, &shards[j].shard_id).await;

                        if merge_result.is_ok() {
                            println!(" &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&& SUCCESSFUL MERGE &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&");
                            // Add the merged shards to the list
                            merged_shards.push(shards[i].shard_id.clone());
                            merged_shards.push(shards[j].shard_id.clone());

                            // Break out of the inner loop after merging
                            break;
                        }
                    }
                    else {
                        println!("%%%%%%%%%%%%%%%%%%%%%%%%% Shards {:?} and {:?} are not adjacent, or other is not active", shards[i].shard_id, shards[j].shard_id);
                    }
                }
            }
        }
        // Loop through the merged shards and set their activity to false
        /*for shard_id in merged_shards {
            { // Acquire a write lock
                let mut shard_data = self.shard_data.write().await;
                let shard = match shard_data.get_shard_by_id_mut(shard_id.clone()) {
                    Some(s) => s,
                    None => {
                        println!("Shard {} not found", shard_id);
                        return Err(WorkerError::ValidationError("Shard not found".to_string()));
                    },
                };
                shard.is_active = false;
            } // Release the write lock
        };*/
        // Update the shard data store list to get the newly merged shards
        // and set old ones to inactive
        match self.update_shard_data_store_list().await {
            Ok(_) => println!(" Shard data store updated"),
            Err(e) => println!("Error updating shard data store: {:?}", e),
        }

        println!("%%%%%%%%%%%%%%%%%%%%%%%%% Exiting merging loop");
        Ok(())
    }

    // Check to see if shards are adjacent
    fn is_adjacent(shard_a: &ShardMetrics, shard_b: &ShardMetrics) -> bool {
        // Check if the end of shard_a matches the start of shard_b
        println!(" ¤¤¤¤¤¤¤¤¤¤ END SHARD i {:?} START SHARD j {:?}", shard_a.ending_hash_key, shard_b.starting_hash_key);
        println!(" ¤¤¤¤¤¤¤¤¤¤ END SHARD j {:?} START SHARD i {:?}", shard_b.ending_hash_key, shard_a.starting_hash_key);
        // First parse both keys to BigInt
        let starting_key_a = match shard_a.starting_hash_key.parse::<BigInt>() {
            Ok(key) => key,
            Err(_) => {
                println!("Error parsing starting key A");
                return false;
            }
        };
        let ending_key_a = match shard_a.ending_hash_key.parse::<BigInt>() {
            Ok(key) => key,
            Err(_) => {
                println!("Error parsing ending key A");
                return false;
            }
        };
        let starting_key_b = match shard_b.starting_hash_key.parse::<BigInt>() {
            Ok(key) => key,
            Err(_) => {
                println!("Error parsing starting key B");
                return false;
            }
        };
        let ending_key_b = match shard_b.ending_hash_key.parse::<BigInt>() {
            Ok(key) => key,
            Err(_) => {
                println!("Error parsing ending key B");
                return false;
            }
        };
        let is_adjacent = ending_key_a + 1 == starting_key_b
            || ending_key_b + 1 == starting_key_a;
        
        println!(" ¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤ IS ADJACENT {:?}", is_adjacent);
        is_adjacent
    }

    async fn merge_shard(&mut self, shard_id_1: &String, shard_id_2: &String) -> Result<(), WorkerError> {
        // Use the Kinesis client to merge the shard with its adjacent shard
        let merge_result = self.kinesis_client
            .merge_shards()
            .stream_name("events")
            .shard_to_merge(shard_id_1.clone())
            .adjacent_shard_to_merge(shard_id_2.clone()) // Define your adjacent shard
            .send()
            .await;
    
        match merge_result {
            Ok(_) => {
                println!("Successfully merged shards: {:?} {:?}", shard_id_1, shard_id_2);
                Ok(())
            },
            Err(err) => {
                println!("Error merging shard: {:?}", err);
                Err(WorkerError::WritingError(vec![err.to_string()]))
            },
        }
    }

}

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

    // ShardDataStore for recording metrics
    // A shared resource between TrafficMonitor and SubmissionWorkers
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