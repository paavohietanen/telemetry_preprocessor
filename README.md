# Telemetry Processor

## Table of Contents

- [Overview](#Overview)
- [Technology Stack](#Technology Stack)
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Features](#features)

## Overview

The **Telemetry Processor** is a command-line tool designed to read, process, and write simulated telemetry data in a single pipeline.

It consists of three main modules:

- **Submission Workers**: These read messages from the SQS queue, process them, and write the results to a Kinesis stream.
    
- **Shard Data Store**: This module stores information about shards and provides the best shards for writing to the Kinesis stream in real time.
    
- **Traffic Monitor**: This background module monitors writing traffic by analyzing data from the Shard Data Store. It assesses shard health and manages both shards in Kinesis and local shard metric entries in the Shard Data Store.

## ## Technology Stack

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