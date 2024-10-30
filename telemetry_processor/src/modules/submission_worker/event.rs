use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {

    // List of new process events
    pub new_process: Vec<NewProcess>,

    // List of network connection events
    pub network_connection: Vec<NetworkConnection>
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewProcess {

    // Command line of the process
    cmdl: String,

    // Username that started the process
    user: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConnection {

    // IPv4
    source_ip: String,

    // IpV4
    destination_ip: String,

    // Range of 0-65535
    destination_port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventType {

    // Event type for new process
    NewProcess(NewProcess),
    
    // Event type for network connection
    NetworkConnection(NetworkConnection),
}

// EventWrapper struct to hold single processed events
#[derive(Debug, Serialize, Deserialize)]
pub struct EventWrapper {

    // Event type with its corresponding data
    pub event_type: EventType,

    // Unique identifier of the event
    pub event_id: String,

    // Unique identifier for the submission event belongs to
    pub submission_id: String,

    // Order number in submission
    pub order: u32,

    // Creation time of the submission, device local time
    pub time_created: String,

    // Processing time of the event, application local time
    pub time_processed: String,
}