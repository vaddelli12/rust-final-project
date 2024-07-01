//! This module provides utility functions for parsing fields.
//! It includes a generic `parse_field` function that attempts to parse a string into a specified type.

/// Parses a string field into a specified type.
///
/// # Arguments
///
/// * `field` - A string slice representing the field to be parsed.
///
/// # Returns
///
/// An `Option` containing the parsed value if successful, or `None` if parsing fails.
///
/// # Type Parameters
///
/// * `T` - The type to parse the string into. This type must implement the `FromStr` trait.
pub fn parse_field<T: std::str::FromStr>(field: &str) -> Option<T> {
    field.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_field_to_i64() {
        assert_eq!(parse_field::<i64>("123"), Some(123));
        assert_eq!(parse_field::<i64>("abc"), None); // Invalid number
        assert_eq!(parse_field::<i64>(""), None); // Empty string
    }

    #[test]
    fn test_parse_field_to_f64() {
        assert_eq!(parse_field::<f64>("123.45"), Some(123.45));
        assert_eq!(parse_field::<f64>("abc"), None); // Invalid number
        assert_eq!(parse_field::<f64>(""), None); // Empty string
    }

    #[test]
    fn test_parse_field_to_string() {
        assert_eq!(parse_field::<String>("test"), Some("test".to_string()));
        assert_eq!(parse_field::<String>(""), Some("".to_string())); // Empty string
    }

    #[test]
    fn test_parse_field_to_bool() {
        assert_eq!(parse_field::<bool>("true"), Some(true));
        assert_eq!(parse_field::<bool>("false"), Some(false));
        assert_eq!(parse_field::<bool>("yes"), None); // Invalid boolean
    }
}
