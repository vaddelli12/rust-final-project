//! This module provides functions to interact with a PostgreSQL database using the `tokio_postgres` crate.
//! It includes functions to connect to the database, create a table, insert movie records, clear the database,
//! retrieve and print the first few records, and process a list of transformed movies.

use crate::models::TransformedMovie;
use tokio_postgres::{Client, Error as PgError, NoTls};

/// Connects to the PostgreSQL database.
///
/// # Returns
///
/// A `Result` containing a `Client` for database interaction or a `PgError`.
pub async fn connect_db() -> Result<Client, PgError> {
    let db_url = "postgres://postgres:postgres@localhost:5432/postgres";
    let (client, connection) = tokio_postgres::connect(&db_url, NoTls).await?;

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    Ok(client)
}

/// Creates the `Movie` table in the PostgreSQL database if it does not exist.
///
/// # Arguments
///
/// * `client` - A reference to a `Client` for database interaction.
///
/// # Returns
///
/// A `Result` indicating success or a `PgError`.
pub async fn create_table(client: &Client) -> Result<(), PgError> {
    client
        .execute(
            "CREATE TABLE IF NOT EXISTS Movie (
            filmtv_id BIGINT PRIMARY KEY,
            title TEXT NOT NULL,
            year BIGINT NOT NULL,
            genre TEXT NOT NULL,
            duration BIGINT NOT NULL,
            country TEXT NOT NULL,
            avg_vote DOUBLE PRECISION NOT NULL,
            critics_vote DOUBLE PRECISION NOT NULL,
            public_vote DOUBLE PRECISION NOT NULL,
            total_votes BIGINT NOT NULL
        )",
            &[],
        )
        .await?;

    Ok(())
}

/// Inserts a list of `TransformedMovie` structs into the `Movie` table in the PostgreSQL database.
///
/// # Arguments
///
/// * `client` - A reference to a `Client` for database interaction.
/// * `movies` - A slice of `TransformedMovie` structs to be inserted.
///
/// # Returns
///
/// A `Result` indicating success or a `PgError`.
pub async fn insert_movies(client: &Client, movies: &[TransformedMovie]) -> Result<(), PgError> {
    for movie in movies {
        client.execute(
            "INSERT INTO Movie (filmtv_id, title, year, genre, duration, country, avg_vote, critics_vote, public_vote, total_votes)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
             ON CONFLICT (filmtv_id) DO UPDATE SET
             title = EXCLUDED.title,
             year = EXCLUDED.year,
             genre = EXCLUDED.genre,
             duration = EXCLUDED.duration,
             country = EXCLUDED.country,
             avg_vote = EXCLUDED.avg_vote,
             critics_vote = EXCLUDED.critics_vote,
             public_vote = EXCLUDED.public_vote,
             total_votes = EXCLUDED.total_votes",
            &[
                &movie.filmtv_id, &movie.title, &movie.year, &movie.genre,
                &movie.duration, &movie.country, &movie.avg_vote,
                &movie.critics_vote, &movie.public_vote, &movie.total_votes,
            ],
        ).await?;
    }

    Ok(())
}

/// Clears the `Movie` table in the PostgreSQL database.
///
/// # Arguments
///
/// * `client` - A reference to a `Client` for database interaction.
///
/// # Returns
///
/// A `Result` indicating success or a `PgError`.
pub async fn cleardb(client: &Client) -> Result<(), PgError> {
    client.execute("DROP TABLE IF EXISTS Movie", &[]).await?;
    Ok(())
}

/// Retrieves and prints the first few records from the `Movie` table in the PostgreSQL database.
///
/// # Arguments
///
/// * `client` - A reference to a `Client` for database interaction.
/// * `limit` - The number of records to retrieve and print.
///
/// # Returns
///
/// A `Result` indicating success or a `PgError`.
pub async fn get_and_print_first_records(client: &Client, limit: i64) -> Result<(), PgError> {
    let rows = client
        .query("SELECT * FROM Movie ORDER BY filmtv_id LIMIT $1", &[&limit])
        .await?;

    println!("First {} records from the database:", limit);
    for row in rows {
        println!("{:?}", row);
    }

    Ok(())
}

/// Processes a list of transformed movies by connecting to the database, clearing any existing data,
/// creating the `Movie` table, inserting the movies, and printing the first few records.
///
/// # Arguments
///
/// * `transformed_movies` - A vector of `TransformedMovie` structs to be processed.
///
/// # Returns
///
/// A `Result` indicating success or an error.
pub async fn process_movies(
    transformed_movies: Vec<TransformedMovie>,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = connect_db().await?;

    cleardb(&client).await?;
    create_table(&client).await?;
    insert_movies(&client, &transformed_movies).await?;

    // Print the first 2 records after insertion
    get_and_print_first_records(&client, 2).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;

    #[tokio::test]
    async fn test_connect_db() {
        // Attempt to connect to the database
        match connect_db().await {
            Ok(client) => {
                // Perform a simple query to check if the connection works
                match client.query("SELECT 1", &[]).await {
                    Ok(_) => {
                        // If the query succeeds, the connection is working
                        assert!(true);
                    }
                    Err(e) => {
                        // If the query fails, print the error
                        panic!("Query failed: {:?}", e);
                    }
                }
            }
            Err(e) => {
                // If the connection fails, print the error
                panic!("Failed to connect to the database: {:?}", e);
            }
        }
    }
}
