//! This module provides functions to transform movie data from a list of `Movie` structs
//! to a list of `TransformedMovie` structs. The transformation process includes converting
//! the data to a DataFrame, cleaning and validating the data, and then converting it back
//! to the desired format.

use crate::models::{Movie, TransformedMovie};
use polars::prelude::*;
use std::error::Error;

/// Transforms a list of `Movie` structs into a list of `TransformedMovie` structs.
///
/// # Arguments
///
/// * `movies` - A vector of `Movie` structs to be transformed.
///
/// # Returns
///
/// A `Result` containing a vector of `TransformedMovie` structs or an error.
pub fn transform_movies(movies: Vec<Movie>) -> Result<Vec<TransformedMovie>, Box<dyn Error>> {
    // Convert Vec<Movie> to DataFrame
    let df = movies_to_dataframe(movies)?;

    // Clean and validate data
    let cleaned_df = clean_and_validate_data(df)?;

    // Convert cleaned DataFrame to Vec<TransformedMovie>
    let transformed_movies = dataframe_to_transformed_movies(&cleaned_df)?;

    Ok(transformed_movies)
}

/// Converts a vector of `Movie` structs into a Polars `DataFrame`.
///
/// # Arguments
///
/// * `movies` - A vector of `Movie` structs to be converted.
///
/// # Returns
///
/// A `Result` containing a `DataFrame` or a `PolarsError`.
fn movies_to_dataframe(movies: Vec<Movie>) -> Result<DataFrame, PolarsError> {
    let filmtv_id = movies.iter().map(|m| m.filmtv_id).collect::<Vec<_>>();
    let title = movies.iter().map(|m| m.title.clone()).collect::<Vec<_>>();
    let year = movies.iter().map(|m| m.year).collect::<Vec<_>>();
    let genre = movies.iter().map(|m| m.genre.clone()).collect::<Vec<_>>();
    let duration = movies.iter().map(|m| m.duration).collect::<Vec<_>>();
    let country = movies.iter().map(|m| m.country.clone()).collect::<Vec<_>>();
    let avg_vote = movies.iter().map(|m| m.avg_vote).collect::<Vec<_>>();
    let critics_vote = movies.iter().map(|m| m.critics_vote).collect::<Vec<_>>();
    let public_vote = movies.iter().map(|m| m.public_vote).collect::<Vec<_>>();
    let total_votes = movies.iter().map(|m| m.total_votes).collect::<Vec<_>>();

    DataFrame::new(vec![
        Series::new("filmtv_id", filmtv_id),
        Series::new("title", title),
        Series::new("year", year),
        Series::new("genre", genre),
        Series::new("duration", duration),
        Series::new("country", country),
        Series::new("avg_vote", avg_vote),
        Series::new("critics_vote", critics_vote),
        Series::new("public_vote", public_vote),
        Series::new("total_votes", total_votes),
    ])
}

/// Cleans and validates the data in a `DataFrame`.
///
/// # Arguments
///
/// * `df` - A `DataFrame` to be cleaned and validated.
///
/// # Returns
///
/// A `Result` containing a cleaned `DataFrame` or a `PolarsError`.
fn clean_and_validate_data(df: DataFrame) -> Result<DataFrame, PolarsError> {
    let cleaned_df = df
        .lazy()
        .with_column(col("filmtv_id").fill_null(lit(0)))
        .with_column(col("title").fill_null(lit("Unknown")))
        .with_column(col("year").fill_null(lit(0)))
        .with_column(col("genre").fill_null(lit("Unknown")))
        .with_column(col("duration").fill_null(lit(0)))
        .with_column(col("country").fill_null(lit("Unknown")))
        .with_column(col("avg_vote").fill_null(lit(0.0)))
        .with_column(col("critics_vote").fill_null(lit(0.0)))
        .with_column(col("public_vote").fill_null(lit(0.0)))
        .with_column(col("total_votes").fill_null(lit(0)))
        .filter(col("filmtv_id").gt(0))
        .collect()?;

    Ok(cleaned_df)
}

/// Converts a cleaned `DataFrame` into a vector of `TransformedMovie` structs.
///
/// # Arguments
///
/// * `df` - A reference to a cleaned `DataFrame`.
///
/// # Returns
///
/// A `Result` containing a vector of `TransformedMovie` structs or an error.
fn dataframe_to_transformed_movies(
    df: &DataFrame,
) -> Result<Vec<TransformedMovie>, Box<dyn Error>> {
    let filmtv_id = df.column("filmtv_id")?.i64()?;
    let title = df.column("title")?.str()?;
    let year = df.column("year")?.i64()?;
    let genre = df.column("genre")?.str()?;
    let duration = df.column("duration")?.i64()?;
    let country = df.column("country")?.str()?;
    let avg_vote = df.column("avg_vote")?.f64()?;
    let critics_vote = df.column("critics_vote")?.f64()?;
    let public_vote = df.column("public_vote")?.f64()?;
    let total_votes = df.column("total_votes")?.i64()?;

    let transformed_movies = (0..df.height())
        .map(|i| {
            Ok(TransformedMovie {
                filmtv_id: filmtv_id.get(i).unwrap_or(0),
                title: title.get(i).unwrap_or("").to_string(),
                year: year.get(i).unwrap_or(0),
                genre: genre.get(i).unwrap_or("").to_string(),
                duration: duration.get(i).unwrap_or(0),
                country: country.get(i).unwrap_or("").to_string(),
                avg_vote: avg_vote.get(i).unwrap_or(0.0),
                critics_vote: critics_vote.get(i).unwrap_or(0.0),
                public_vote: public_vote.get(i).unwrap_or(0.0),
                total_votes: total_votes.get(i).unwrap_or(0),
            })
        })
        .collect::<Result<Vec<TransformedMovie>, Box<dyn Error>>>()?;

    Ok(transformed_movies)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Movie;

    #[test]
    fn test_transform_movies() {
        let movies = vec![
            Movie {
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
            },
            Movie {
                filmtv_id: Some(2),
                title: None,
                year: Some(2020),
                genre: Some("Comedy".to_string()),
                duration: Some(90),
                country: None,
                directors: None,
                actors: None,
                avg_vote: Some(7.5),
                critics_vote: Some(8.0),
                public_vote: Some(7.0),
                total_votes: Some(500),
                description: None,
                notes: None,
                humor: Some(6),
                rhythm: Some(5),
                effort: Some(6),
                tension: Some(4),
                erotism: Some(3),
            },
        ];

        let transformed_movies = transform_movies(movies).unwrap();

        assert_eq!(transformed_movies.len(), 2);
        assert_eq!(transformed_movies[0].filmtv_id, 1);
        assert_eq!(transformed_movies[0].title, "Example Movie".to_string());
        assert_eq!(transformed_movies[0].year, 2021);
        assert_eq!(transformed_movies[0].genre, "Drama".to_string());
        assert_eq!(transformed_movies[0].duration, 120);
        assert_eq!(transformed_movies[0].country, "USA".to_string());
        assert_eq!(transformed_movies[0].avg_vote, 8.5);
        assert_eq!(transformed_movies[0].critics_vote, 9.0);
        assert_eq!(transformed_movies[0].public_vote, 8.0);
        assert_eq!(transformed_movies[0].total_votes, 1000);

        assert_eq!(transformed_movies[1].filmtv_id, 2);
        assert_eq!(transformed_movies[1].title, "Unknown".to_string());
        assert_eq!(transformed_movies[1].year, 2020);
        assert_eq!(transformed_movies[1].genre, "Comedy".to_string());
        assert_eq!(transformed_movies[1].duration, 90);
        assert_eq!(transformed_movies[1].country, "Unknown".to_string());
        assert_eq!(transformed_movies[1].avg_vote, 7.5);
        assert_eq!(transformed_movies[1].critics_vote, 8.0);
        assert_eq!(transformed_movies[1].public_vote, 7.0);
        assert_eq!(transformed_movies[1].total_votes, 500);
    }

    #[test]
    fn test_movies_to_dataframe() {
        let movies = vec![Movie {
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
        }];

        let df = movies_to_dataframe(movies).unwrap();

        assert_eq!(df.shape(), (1, 10));
    }

    #[test]
    fn test_clean_and_validate_data() {
        let movies = vec![
            Movie {
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
            },
            Movie {
                filmtv_id: None,
                title: None,
                year: None,
                genre: None,
                duration: None,
                country: None,
                directors: None,
                actors: None,
                avg_vote: None,
                critics_vote: None,
                public_vote: None,
                total_votes: None,
                description: None,
                notes: None,
                humor: None,
                rhythm: None,
                effort: None,
                tension: None,
                erotism: None,
            },
        ];

        let df = movies_to_dataframe(movies).unwrap();
        let cleaned_df = clean_and_validate_data(df).unwrap();

        assert_eq!(cleaned_df.shape(), (1, 10));
    }

    #[test]
    fn test_dataframe_to_transformed_movies() {
        let movies = vec![Movie {
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
        }];

        let df = movies_to_dataframe(movies).unwrap();
        let cleaned_df = clean_and_validate_data(df).unwrap();
        let transformed_movies = dataframe_to_transformed_movies(&cleaned_df).unwrap();

        assert_eq!(transformed_movies.len(), 1);
        assert_eq!(transformed_movies[0].filmtv_id, 1);
        assert_eq!(transformed_movies[0].title, "Example Movie".to_string());
        assert_eq!(transformed_movies[0].year, 2021);
        assert_eq!(transformed_movies[0].genre, "Drama".to_string());
        assert_eq!(transformed_movies[0].duration, 120);
        assert_eq!(transformed_movies[0].country, "USA".to_string());
        assert_eq!(transformed_movies[0].avg_vote, 8.5);
        assert_eq!(transformed_movies[0].critics_vote, 9.0);
        assert_eq!(transformed_movies[0].public_vote, 8.0);
        assert_eq!(transformed_movies[0].total_votes, 1000);
    }
}
