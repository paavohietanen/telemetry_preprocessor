use std::time::SystemTime;
use crate::modules::error::WorkerError;

// ShardMetrics struct to hold metrics for each shard
#[derive(Debug, Clone)]
pub struct ShardMetrics {
    // Identifier of the shard
    pub shard_id: String,
    // Starting hash key of the shard
    // Needed in shard splitting and merging
    pub starting_hash_key: String,
    // Ending hash key of the shard
    // Needed in shard merging
    pub ending_hash_key: String,
    // Average data processed per entry over TrafficMonitor's
    // observation duration
    pub average_data_processed: f64,
    // Number of ProvisionedThrougputErrors ovre TrafficMonitor's
    // observation duration
    pub recent_throughput_errors: u64,
    // Traffic data entries for this shard
    pub entries: Vec<ShardMetricEntry>,
    // Submissions that are being written to this shard
    pub submissions: Vec<String>,
    // Whether the shard is active or not
    pub is_active: bool,
}

// A single entry of shard data
#[derive(Debug, Clone)]
pub struct ShardMetricEntry {
    pub timestamp: SystemTime,
    pub data_processed: Option<u64>,
    pub records_read: Option<u64>,
    pub throughput_errors: Option<u64>,
}

pub struct ShardDataStore {
    pub shards: Vec<ShardMetrics>,
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

    // Add a submission id to the shard metric to mark it as being written to the respective shard
    pub fn add_submission(&mut self, shard_id: &str, submission_id: &str) {
        // Find the shard metric in the shards vector
        let shard = self.shards.iter_mut().find(|s| s.shard_id == shard_id);

        // If the shard metric is found, add the submission id
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
