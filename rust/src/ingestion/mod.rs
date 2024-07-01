//! This module provides functionality to ingest movie data from a CSV file.
//! It includes the `ingest_csv` function which reads a CSV file and converts it into a vector of `Movie` structs.

use std::fs::File;
use std::io::{self};
use std::path::Path;

use crate::models::{IngestionError, Movie};
use crate::utils::parse_field;

/// Ingests movie data from a CSV file and converts it into a vector of `Movie` structs.
///
/// # Arguments
///
/// * `path` - A path to the CSV file.
///
/// # Returns
///
/// A `Result` containing a vector of `Movie` structs or an `IngestionError`.
pub fn ingest_csv<P: AsRef<Path>>(path: P) -> Result<Vec<Movie>, IngestionError> {
    let file = File::open(path)?;
    let reader = io::BufReader::new(file);
    let mut csv_reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(reader);

    let headers = csv_reader.headers()?.clone();

    let movies: Result<Vec<Movie>, IngestionError> = csv_reader
        .records()
        .enumerate()
        .map(|(index, record)| {
            let record = record?;
            let mut movie_data = serde_json::Map::new();

            // Parse each field in the record and add it to movie_data
            for (i, field) in record.iter().enumerate() {
                let header = headers.get(i).unwrap_or("unknown");
                let value = match header {
                    "filmtv_id" | "year" | "duration" | "total_votes" | "humor" | "rhythm"
                    | "effort" | "tension" | "erotism" => {
                        if let Some(num) = parse_field::<i64>(field) {
                            serde_json::Value::Number(num.into())
                        } else {
                            serde_json::Value::Null
                        }
                    }
                    "avg_vote" | "critics_vote" | "public_vote" => {
                        if let Some(num) = parse_field::<f64>(field) {
                            serde_json::Value::Number(serde_json::Number::from_f64(num).unwrap())
                        } else {
                            serde_json::Value::Null
                        }
                    }
                    _ => serde_json::Value::String(field.to_string()),
                };
                movie_data.insert(header.to_string(), value);
            }

            // Convert the JSON object to a Movie struct
            let movie: Movie = serde_json::from_value(serde_json::Value::Object(movie_data))
                .map_err(|e| {
                    println!("Error parsing row {}: {:?}", index, e);
                    IngestionError::DeserializationError(e)
                })?;

            Ok(movie)
        })
        .collect();

    movies
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_ingest_csv() {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test_movies.csv");

        let mut file = File::create(&file_path).unwrap();
        writeln!(
            file,
            "filmtv_id,title,year,genre,duration,country,avg_vote,critics_vote,public_vote,total_votes,humor,rhythm,effort,tension,erotism"
        ).unwrap();
        writeln!(
            file,
            "1,Example Movie,2021,Drama,120,USA,8.5,9.0,8.0,1000,5,6,7,8,4"
        )
        .unwrap();
        writeln!(
            file,
            "2,Another Movie,2020,Comedy,90,UK,7.5,8.0,7.0,500,6,5,6,4,3"
        )
        .unwrap();

        let movies = ingest_csv(&file_path).unwrap();

        assert_eq!(movies.len(), 2);
        assert_eq!(movies[0].filmtv_id, Some(1));
        assert_eq!(movies[0].title, Some("Example Movie".to_string()));
        assert_eq!(movies[0].year, Some(2021));
        assert_eq!(movies[0].genre, Some("Drama".to_string()));
        assert_eq!(movies[0].duration, Some(120));
        assert_eq!(movies[0].country, Some("USA".to_string()));
        assert_eq!(movies[0].avg_vote, Some(8.5));
        assert_eq!(movies[0].critics_vote, Some(9.0));
        assert_eq!(movies[0].public_vote, Some(8.0));
        assert_eq!(movies[0].total_votes, Some(1000));
        assert_eq!(movies[0].humor, Some(5));
        assert_eq!(movies[0].rhythm, Some(6));
        assert_eq!(movies[0].effort, Some(7));
        assert_eq!(movies[0].tension, Some(8));
        assert_eq!(movies[0].erotism, Some(4));

        assert_eq!(movies[1].filmtv_id, Some(2));
        assert_eq!(movies[1].title, Some("Another Movie".to_string()));
        assert_eq!(movies[1].year, Some(2020));
        assert_eq!(movies[1].genre, Some("Comedy".to_string()));
        assert_eq!(movies[1].duration, Some(90));
        assert_eq!(movies[1].country, Some("UK".to_string()));
        assert_eq!(movies[1].avg_vote, Some(7.5));
        assert_eq!(movies[1].critics_vote, Some(8.0));
        assert_eq!(movies[1].public_vote, Some(7.0));
        assert_eq!(movies[1].total_votes, Some(500));
        assert_eq!(movies[1].humor, Some(6));
        assert_eq!(movies[1].rhythm, Some(5));
        assert_eq!(movies[1].effort, Some(6));
        assert_eq!(movies[1].tension, Some(4));
        assert_eq!(movies[1].erotism, Some(3));
    }

    #[test]
    fn test_ingest_csv_with_missing_values() {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test_movies_with_missing_values.csv");

        let mut file = File::create(&file_path).unwrap();
        writeln!(
            file,
            "filmtv_id,title,year,genre,duration,country,avg_vote,critics_vote,public_vote,total_votes,humor,rhythm,effort,tension,erotism"
        ).unwrap();
        writeln!(
            file,
            "1,Example Movie,2021,Drama,120,USA,8.5,9.0,8.0,1000,5,6,7,8,4"
        )
        .unwrap();
        writeln!(
            file,
            "2,Another Movie,,Comedy,90,,7.5,8.0,7.0,500,6,5,6,4,3"
        )
        .unwrap();

        let movies = ingest_csv(&file_path).unwrap();

        assert_eq!(movies.len(), 2);
        assert_eq!(movies[1].filmtv_id, Some(2));
        assert_eq!(movies[1].title, Some("Another Movie".to_string()));
        assert_eq!(movies[1].year, None);
        assert_eq!(movies[1].genre, Some("Comedy".to_string()));
        assert_eq!(movies[1].duration, Some(90));
        assert_eq!(movies[1].country, Some("".to_string()));
        assert_eq!(movies[1].avg_vote, Some(7.5));
        assert_eq!(movies[1].critics_vote, Some(8.0));
        assert_eq!(movies[1].public_vote, Some(7.0));
        assert_eq!(movies[1].total_votes, Some(500));
        assert_eq!(movies[1].humor, Some(6));
        assert_eq!(movies[1].rhythm, Some(5));
        assert_eq!(movies[1].effort, Some(6));
        assert_eq!(movies[1].tension, Some(4));
        assert_eq!(movies[1].erotism, Some(3));
    }
}
