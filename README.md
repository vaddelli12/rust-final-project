# Scalable Data Engineering Pipeline in Rust

## Overview

This project demonstrates the capabilities of Rust in designing and implementing a scalable and efficient data engineering pipeline. The pipeline covers the complete ETL (Extract, Transform, Load) process and includes data ingestion, data transformation, storage, and visualization. 

## Project Structure

```
.
├── README.md
├── python.ipynb
└── rust
    ├── Cargo.lock
    ├── Cargo.toml
    ├── dataset
    │   └── filmtv_movies.csv
    ├── docs
    ├── src
    │   ├── db
    │   │   └── mod.rs
    │   ├── ingestion
    │   │   └── mod.rs
    │   ├── main.rs
    │   ├── models
    │   │   └── mod.rs
    │   ├── transform
    │   │   └── mod.rs
    │   └── utils
    │       └── mod.rs
```

## Features

- **Data Ingestion**: Collect data from multiple CSV files with error handling and retry logic.
- **Data Transformation**: Clean, normalize, and validate data using Rust libraries like Polars for efficient data manipulation.
- **Storage**: Store the processed data in PostgreSQL
- **Visualization**: Visualize the data using Python (Matplotlib, Seaborn etc).
- **Error Handling**: Comprehensive error handling using Rust's `Result` enum.
- **Unit Testing**: Unit tests for all major components to ensure reliability and correctness.
- **Documentation**: Detailed documentation for understanding and extending the project.

## Requirements

- Rust
- Python > 3.9
- PostgreSQL

## Quick Start

1. **Clone the repository**:

    ```sh
    git clone https://github.com/vaddelli12/rust-final-project
    cd rust-final-project/rust
    ```

2. **Build the project**:

    ```sh
    cargo build
    ```

3. **Run the project**:

    ```sh
    cargo run
    ```

4. **Unit Tests**:

    ```sh
    cargo test
    ```

## Configuration

### Database Configuration

To configure PostgreSQL, update the `DATABASE_URL` in your environment variables:

```sh
export DATABASE_URL=postgres://user:password@localhost/dbname
```

## Documentation

Detailed documentation is available in the `docs` directory. You can generate the latest documentation using:

```sh
cargo doc --no-deps --open
```
