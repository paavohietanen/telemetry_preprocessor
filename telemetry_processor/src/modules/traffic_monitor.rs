use tokio::{
    sync::RwLock,
    time::{
        Duration,
        sleep,
    },
};
use std::{
    sync::Arc,
    time::SystemTime,
};
use num_bigint::BigInt;

use crate::{
    config,
    modules::{
        shard_data_store::{
        ShardDataStore,
        ShardMetrics,
        },
        error::WorkerError,
    },
};

pub struct TrafficMonitor {

    // Kinesis client to fetch shards
    kinesis_client: aws_sdk_kinesis::Client,

    // ShardDataStore for managing traffic data
    // A shared resource between TrafficMonitor and SubmissionWorkers
    shard_data: Arc<RwLock<ShardDataStore>>,

    // Configuration for TrafficMonitor
    config: config::TrafficMonitorConfig,
}

impl TrafficMonitor {

    pub fn new(kinesis_client: aws_sdk_kinesis::Client,
        shard_data: Arc<RwLock<ShardDataStore>>,
        config: config::TrafficMonitorConfig) -> Self {
        // Load the configuration for SubmissionWorker and TrafficMonitor from Config.toml
        Self {
            kinesis_client,
            shard_data,
            config,
        }
    }

    // Function to start monitoring traffic
    pub async fn start_monitoring(&mut self) {

        // First, update the shard data of the shard data store
        match self.update_shard_data_store_list().await {
            Ok(_) => println!(" Shard data store updated"),
            // TODO: Check if requires better handling
            Err(e) => println!("Error updating shard data store: {:?}", e),
        };

        // Start the monitoring loop
        loop {
            // Check traffic conditions
            self.manage_traffic().await;
            sleep(Duration::from_secs(self.config.interval_length)).await;
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
            let mut request = self.kinesis_client.list_shards();
            

            // Only add exclusive_start_shard_id to the request if next_token is Some
            if let Some(ref token) = next_token {
                request = request.next_token(token); // Use token directly
            }
            else {
                // Use stream name directly if no token. If we have both, there will be an InvalidArgumentException
                request = request.stream_name("events"); 
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
                        elapsed <= tokio::time::Duration::from_secs(self.config.entry_lifetime)
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

                        if elapsed.as_secs() <= self.config.observation_period {
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
                let usage_percentage = (metrics.average_data_processed as f64) / self.config.max_capacity * 100.0;

                // Assign the throughput errors to the shard metrics object
                metrics.recent_throughput_errors = throughput_errors;

                // If the usage percentage or the amount of recent throughput errors exceed their thresholds,
                // assign to be split
                if (usage_percentage >= self.config.split_usage_threshold * 100.0)
                    || (metrics.recent_throughput_errors >= self.config.split_tp_error_threshold) {
                    println!(" ## Shard {} is over 80% capacity, assigning for splitting...", metrics.shard_id);
                    shards_to_split.push(metrics.shard_id.clone());
                }

                // If the usage percentage is below or equal to the merge threshold, and there have been no
                // recent throughput errors, assign as a merge candidate
                if (usage_percentage <= self.config.merge_usage_threshold * 100.0) 
                    && (metrics.recent_throughput_errors <= self.config.merge_tp_error_threshold) {
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
            Err(_) => return Err(WorkerError::ValidationError("Error parsing starting key".to_string())),
        };
        println!("Starting key: {:?}", starting_key);
        println!("Ending key: {:?}", original_ending_hash_key);
        let ending_key: BigInt = match original_ending_hash_key.parse::<BigInt>() {
            Ok(key) => key,
            Err(_) => return Err(WorkerError::ValidationError("Error parsing ending key".to_string()))
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
        println!("% Merging shards...");
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
                    println!(" %% Activity shard i {:?} {:?} shard j {:?} {:?}", shards[i].shard_id, shards[i].is_active, shards[j].shard_id, shards[j].is_active);
                    if shards[j].is_active && Self::is_adjacent(&shards[i], &shards[j]) {
                        println!(" %%% Merging shards: {:?} {:?}", shards[i].shard_id, shards[j].shard_id);
                        let merge_result = &self.merge_shard(&shards[i].shard_id, &shards[j].shard_id).await;

                        if merge_result.is_ok() {
                            println!(" %%%% Successful merge");
                            // Add the merged shards to the list
                            merged_shards.push(shards[i].shard_id.clone());
                            merged_shards.push(shards[j].shard_id.clone());

                            // Break out of the inner loop after merging
                            break;
                        }
                    }
                    else {
                        println!(" %%%% Shards {:?} and {:?} are not adjacent, or other is not active", shards[i].shard_id, shards[j].shard_id);
                    }
                }
            }
        }
        // Update the shard data store list to get the newly merged shards
        // and set old ones to inactive
        match self.update_shard_data_store_list().await {
            Ok(_) => println!(" Shard data store updated"),
            Err(e) => println!("Error updating shard data store: {:?}", e),
        }

        println!("%%%%% Concluding merge");
        Ok(())
    }

    // Check to see if shards are adjacent
    fn is_adjacent(shard_a: &ShardMetrics, shard_b: &ShardMetrics) -> bool {
        // Check if the end of shard_a matches the start of shard_b
        println!(" ¤ End shard i {:?}, start shard j {:?}", shard_a.ending_hash_key, shard_b.starting_hash_key);
        println!(" ¤ Start shard i {:?}, end shard j {:?} ", shard_a.starting_hash_key, shard_b.ending_hash_key);
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
        
        println!(" ¤¤ Adjacency is {:?}", is_adjacent);
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