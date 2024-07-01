//! This is the main module for the data pipeline application.
//! It orchestrates the ingestion, transformation, and database insertion of movie data.

use anyhow::Result;

mod db;
mod ingestion;
mod models;
mod transform;
mod utils;

#[tokio::main]
async fn main() -> Result<()> {
    let dataset_path = "dataset/filmtv_movies.csv";

    // Ingest movie data from the CSV file
    let df = match ingestion::ingest_csv(dataset_path) {
        Ok(df) => {
            println!("DataFrame loaded successfully.");
            println!("First row:");
            for movie in df.iter().take(1) {
                println!("{:?}", movie);
            }
            df
        }
        Err(e) => {
            eprintln!("Failed to load DataFrame: {:?}", e);
            return Ok(());
        }
    };

    // Transform the ingested movie data
    let transformed_movies = match transform::transform_movies(df) {
        Ok(transformed_movies) => {
            println!("Movies transformed successfully.");
            println!("First transformed movie:");
            if let Some(first_transformed) = transformed_movies.first() {
                println!("{:?}", first_transformed);
            }
            println!(
                "Total number of transformed movies: {}",
                transformed_movies.len()
            );
            transformed_movies
        }
        Err(e) => {
            eprintln!("Failed to transform movies: {:?}", e);
            return Ok(());
        }
    };

    println!("Movies transformed successfully.");

    // Create a new list with only the first 10 transformed movies
    // let transformed_movies = transformed_movies.into_iter().take(10).collect();

    // Process the transformed movies (insert into database)
    match db::process_movies(transformed_movies).await {
        Ok(_) => println!("Movies successfully inserted into database."),
        Err(e) => eprintln!("Error inserting movies into database: {:?}", e),
    }

    println!("Data pipeline finished successfully.");

    Ok(())
}
