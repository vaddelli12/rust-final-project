//! This module provides structures and error handling for ingesting and transforming movie data.
//! It includes definitions for `IngestionError`, `Movie`, and `TransformedMovie` structs.

use serde::Deserialize;
use std::io::{self};

/// Enum representing various errors that can occur during the ingestion process.
#[derive(Debug)]
pub enum IngestionError {
    /// Error that occurs during I/O operations.
    IoError(io::Error),
    /// Error that occurs during CSV parsing.
    CsvError(csv::Error),
    /// Error that occurs during JSON deserialization.
    DeserializationError(serde_json::Error),
}

impl From<io::Error> for IngestionError {
    /// Converts an `io::Error` into an `IngestionError`.
    fn from(err: io::Error) -> Self {
        IngestionError::IoError(err)
    }
}

impl From<csv::Error> for IngestionError {
    /// Converts a `csv::Error` into an `IngestionError`.
    fn from(err: csv::Error) -> Self {
        IngestionError::CsvError(err)
    }
}

impl From<serde_json::Error> for IngestionError {
    /// Converts a `serde_json::Error` into an `IngestionError`.
    fn from(err: serde_json::Error) -> Self {
        IngestionError::DeserializationError(err)
    }
}

/// Struct representing a movie with optional fields.
/// This struct is used for deserializing movie data from various sources.
#[derive(Debug, Deserialize)]
pub struct Movie {
    pub filmtv_id: Option<i64>,
    pub title: Option<String>,
    pub year: Option<i64>,
    pub genre: Option<String>,
    pub duration: Option<i64>,
    pub country: Option<String>,
    pub directors: Option<String>,
    pub actors: Option<String>,
    pub avg_vote: Option<f64>,
    pub critics_vote: Option<f64>,
    pub public_vote: Option<f64>,
    pub total_votes: Option<i64>,
    pub description: Option<String>,
    pub notes: Option<String>,
    pub humor: Option<i64>,
    pub rhythm: Option<i64>,
    pub effort: Option<i64>,
    pub tension: Option<i64>,
    pub erotism: Option<i64>,
}

/// Struct representing a transformed movie with non-optional fields.
/// This struct is used for storing movie data after transformation.
#[derive(Debug)]
pub struct TransformedMovie {
    pub filmtv_id: i64,
    pub title: String,
    pub year: i64,
    pub genre: String,
    pub duration: i64,
    pub country: String,
    pub avg_vote: f64,
    pub critics_vote: f64,
    pub public_vote: f64,
    pub total_votes: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_movie_creation() {
        let movie = Movie {
            filmtv_id: Some(1),
            title: Some("Example Movie".to_string()),
            year: Some(2021),
            genre: Some("Drama".to_string()),
            duration: Some(120),
            country: Some("USA".to_string()),
            directors: Some("John Doe".to_string()),
            actors: Some("Jane Doe".to_string()),
            avg_vote: Some(8.5),
            critics_vote: Some(9.0),
            public_vote: Some(8.0),
            total_votes: Some(1000),
            description: Some("An example movie description.".to_string()),
            notes: Some("Some notes.".to_string()),
            humor: Some(5),
            rhythm: Some(6),
            effort: Some(7),
            tension: Some(8),
            erotism: Some(4),
        };

        assert_eq!(movie.filmtv_id, Some(1));
        assert_eq!(movie.title, Some("Example Movie".to_string()));
        assert_eq!(movie.year, Some(2021));
        assert_eq!(movie.genre, Some("Drama".to_string()));
        assert_eq!(movie.duration, Some(120));
        assert_eq!(movie.country, Some("USA".to_string()));
        assert_eq!(movie.directors, Some("John Doe".to_string()));
        assert_eq!(movie.actors, Some("Jane Doe".to_string()));
        assert_eq!(movie.avg_vote, Some(8.5));
        assert_eq!(movie.critics_vote, Some(9.0));
        assert_eq!(movie.public_vote, Some(8.0));
        assert_eq!(movie.total_votes, Some(1000));
        assert_eq!(
            movie.description,
            Some("An example movie description.".to_string())
        );
        assert_eq!(movie.notes, Some("Some notes.".to_string()));
        assert_eq!(movie.humor, Some(5));
        assert_eq!(movie.rhythm, Some(6));
        assert_eq!(movie.effort, Some(7));
        assert_eq!(movie.tension, Some(8));
        assert_eq!(movie.erotism, Some(4));
    }

    #[test]
    fn test_transformed_movie_creation() {
        let transformed_movie = TransformedMovie {
            filmtv_id: 1,
            title: "Example Movie".to_string(),
            year: 2021,
            genre: "Drama".to_string(),
            duration: 120,
            country: "USA".to_string(),
            avg_vote: 8.5,
            critics_vote: 9.0,
            public_vote: 8.0,
            total_votes: 1000,
        };

        assert_eq!(transformed_movie.filmtv_id, 1);
        assert_eq!(transformed_movie.title, "Example Movie".to_string());
        assert_eq!(transformed_movie.year, 2021);
        assert_eq!(transformed_movie.genre, "Drama".to_string());
        assert_eq!(transformed_movie.duration, 120);
        assert_eq!(transformed_movie.country, "USA".to_string());
        assert_eq!(transformed_movie.avg_vote, 8.5);
        assert_eq!(transformed_movie.critics_vote, 9.0);
        assert_eq!(transformed_movie.public_vote, 8.0);
        assert_eq!(transformed_movie.total_votes, 1000);
    }
}
