use serde::{Serialize, Deserialize};
use crate::modules::submission_worker::event::Event;

#[derive(Debug, Serialize, Deserialize)]
pub struct Submission {
    // Unique identifier for the submission
    pub submission_id: String,
    // Unique identifier for the device
    pub device_id: String,
    // Creation time of the submission, device local time
    pub time_created: String,
    // Corresponding event data
    pub events: Event,
}