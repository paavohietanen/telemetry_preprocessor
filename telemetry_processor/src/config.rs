use serde::Deserialize;
use std::fs::File;
use std::io::Read;
use toml;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub submission_worker: SubmissionWorkerConfig,
    pub traffic_monitor: TrafficMonitorConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SubmissionWorkerConfig {
    pub visibility_timeout: i32,
    pub max_number_of_messages: i32,
    pub polling_interval: u64,
    pub max_retry_attempts: i32,
    pub retry_interval: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TrafficMonitorConfig {
    pub max_capacity: f64,
    pub split_usage_threshold: f64,
    pub split_tp_error_threshold: u64,
    pub merge_usage_threshold: f64,
    pub merge_tp_error_threshold: u64,
    pub observation_period: u64,
    pub interval_length: u64,
    pub entry_lifetime: u64,
}

pub fn load_config(path: &str) -> Result<Config, Box<dyn std::error::Error>> {
    let mut file = File::open(path)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;

    let config: Config = toml::from_str(&contents)?;
    Ok(config)
}