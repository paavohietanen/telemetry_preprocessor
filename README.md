# Telemetry Processor

## Table of Contents

- [Overview](#Overview)
- [Installation](#installation)
- [Technology Stack](#Technology-Stack)
- [Usage](#usage)

## Overview

The **Telemetry Processor** is a command-line tool designed to read, process, and write simulated telemetry data in a single pipeline.

It consists of three main modules:

- **Submission Workers**: These read messages from the SQS queue, process them, and write the results to a Kinesis stream.
    
- **Shard Data Store**: This module stores information about shards and provides the best shards for writing to the Kinesis stream in real time.
    
- **Traffic Monitor**: This background module monitors writing traffic by analyzing data from the Shard Data Store. It assesses shard health and manages both shards in Kinesis and local shard metric entries in the Shard Data Store.

## Installation

### Prerequisites

Before you begin, ensure you have the following installed on your machine:

- **Rust**: Follow the instructions on the official Rust website to install Rust and the Rust package manager, Cargo.
- **Docker**: Install Docker Desktop for your operating system. Ensure Docker is running before proceeding.

### Clone the Repository

Start by cloning the repository to your local machine:

```Bash
git clone <repository-url> cd <repository-directory>
```

### Build the Project

Once you have the repository, you can build the project using Cargo:
```Bash
cargo build
```

### Run LocalStack

To run LocalStack, use Docker with the following command:

```Bash
docker run --rm -it -p 4566:4566 -p 4510-4559:4510-4559 localstack/localstack:3.8.1
```

### Configure and Run the Application

1. **Check the configuration**. Application configuration is in `Config.toml` file in the project root. If you are fine with the default values, move on to next part. Otherwise, modify the file to your needs.

```toml
[submission_worker]

visibility_timeout = 60 # Visibility timeout in seconds for the SQS message

max_number_of_messages = 10 # Maximum number of messages to be received from SQS with one request

polling_interval = 1 # Polling interval in seconds for the SQS queue

max_retry_attempts = 3 # Maximum number of retry attempts for a failed request

retry_interval = 1 # Retry interval in seconds for a failed request

  

[traffic_monitor]

max_capacity = 1_000_000.0 # 1 MB/s capacity of a shard

split_usage_threshold = 0.8 # Value of usage capacity (higher bound) to trigger a shard split

split_tp_error_threshold = 2 # Number of tolerable errors (higher bound) during a monitoring period to trigger a shard split

merge_usage_threshold = 0.001 # Value of usage capacity (lower bound) to trigger a shard merge

merge_tp_error_threshold = 0 # Number of tolerable errors (lower bound) during a monitoring period to trigger a shard merge

observation_period = 10 # Duration of one monitoring period in seconds, from where the traffic monitor will consider the usage and errors

interval_length = 1 # Length of the interval between traffic monitor checks in seconds

entry_lifetime = 60 # Max life time for shard data entries in seconds before the traffic monitor removes them
```

2. **Run the Application**: After creating the configuration file, you can run your application with:
```Bash
    cargo run
```

### Accessing LocalStack

You can interact with LocalStack's services through the AWS CLI or SDKs. Ensure you set the endpoint to `http://localhost:4566` when making requests to SQS and Kinesis.

### Verify Installation

To verify that everything is set up correctly, you can list the SQS queues or Kinesis streams using the AWS CLI:

```Bash
`aws --endpoint-url=http://localhost:4566 sqs list-queues aws --endpoint-url=http://localhost:4566 kinesis list-streams`
```

## Technology Stack

- **Programming Language**: Rust
    - **Compiler**: Rustc 1.82.0
    - **Package Manager**: Cargo 1.82.0

- **Libraries**:
    - **AWS SDKs**:
        - `aws-sdk-kinesis` 1.47.0 — Interacts with Kinesis for data streaming.
        - `aws-sdk-sqs` 1.47.0 — Handles message queueing with SQS.
        - `aws-types` 1.3.3 — Core AWS types for consistent configurations.
    - **Data Handling**:
        - `serde` 1.0, `serde_json` 1.0 — Serializes/deserializes data, especially JSON.
        - `jsonschema` 0.26.0 — Validates JSON structures, needed for submission validation.
        - `base64` 0.22.1 — Encodes and decodes data in Base64 format, needed for submission validation.
    - **Async Processing**:
        - `tokio` 1 — Provides async runtime for handling concurrent operations.
        - `futures` 0.3.31 — Complements async operations with combinators and tools.
    - **Utilities**:
        - `log` 0.4 — Standardized logging across modules.
        - `uuid` 1 — Generates unique identifiers.
        - `chrono` 0.4.38 — Manages and formats dates and times, needed to get the local system time in ISO 8601 format for timestamps.
        - `toml` 0.8.19 — Parses configuration files in TOML format.
        - `num-bigint` 0.4, `num-traits` 0.2 — Provides arbitrary precision integers and number utilities, needed to parse shard hashkeys.
    
- **Services**:
	- **LocalStack** 3.8.1 — Emulates **AWS SQS** and **AWS Kinesis** locally
	- **Docker Desktop** 4.35 — Container environment for running LocalStack

## Usage