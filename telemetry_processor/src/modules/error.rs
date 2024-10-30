use std::fmt;

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