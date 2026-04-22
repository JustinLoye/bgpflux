use std::fmt::Display;

use bgpkit_broker::{load_collectors, BrokerError};
use bgpkit_parser::Filter;
use chrono::{DateTime, NaiveDate, TimeZone, Utc};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataType {
    Update,
    Rib,
    Both,
}

/// Configuration for a BGP stream.
///
/// This struct holds the parameters that define which BGP data to stream:
/// time range, collectors, and data types.
#[derive(Debug, Clone)]
pub struct BgpStreamConfig {
    /// Start timestamp (RFC3339 format or Unix timestamp)
    pub ts_start: String,
    /// End timestamp (RFC3339 format or Unix timestamp)
    pub ts_end: String,
    /// Collector IDs to include in the stream
    pub collectors: Vec<String>,
    /// Type of BGP data to stream
    pub data_type: DataType,
    /// Filter options (same as bgpkit-parser)
    pub filters: Option<Vec<Filter>>,
}

/// Parses various timestamp formats into a UTC DateTime.
///
/// Supports the following formats:
/// - RFC3339 with timezone: `2023-01-01T12:00:00+00:00`
/// - RFC3339 with Z: `2023-01-01T12:00:00Z`
/// - ISO8601: `2023-01-01T12:00:00`
/// - Date only: `2023-01-01`
/// - Unix timestamp: `1672531200`
///
/// # Arguments
///
/// * `timestamp` - A timestamp string in any of the supported formats
///
/// # Returns
///
/// A `DateTime<Utc>` if parsing succeeds, or a `BrokerError` if it fails
///
/// # Example
///
/// ```ignore
/// let dt = parse_timestamp("2023-01-01T12:00:00Z").unwrap();
/// let dt = parse_timestamp("1672531200").unwrap();
/// let dt = parse_timestamp("2023-01-01").unwrap();
/// ```
///
/// Note: This function is adapted from bgpkit-broker.
pub fn parse_timestamp(timestamp: &str) -> Result<DateTime<Utc>, BrokerError> {
    // This function is adapted from bgpkit-broker
    // Original source: https://github.com/bgpkit/bgpkit-broker/
    // Copyright (c) 2021 Mingwei Zhang
    // Licensed under the MIT License

    let ts_str = timestamp.trim();

    // Try parsing as RFC3339 with timezone (including +00:00, -05:00, Z, etc.)
    if let Ok(dt_with_tz) = DateTime::parse_from_rfc3339(ts_str) {
        return Ok(dt_with_tz.with_timezone(&Utc));
    }

    // Try parsing as RFC3339/ISO8601 with Z
    if let Ok(naive_dt) = chrono::NaiveDateTime::parse_from_str(ts_str, "%Y-%m-%dT%H:%M:%SZ") {
        return Ok(Utc.from_utc_datetime(&naive_dt));
    }

    // Try parsing as RFC3339 without Z (assume UTC)
    if let Ok(naive_dt) = chrono::NaiveDateTime::parse_from_str(ts_str, "%Y-%m-%dT%H:%M:%S") {
        return Ok(Utc.from_utc_datetime(&naive_dt));
    }

    // Try parsing as "YYYY-MM-DD HH:MM:SS" (assume UTC)
    if let Ok(naive_dt) = chrono::NaiveDateTime::parse_from_str(ts_str, "%Y-%m-%d %H:%M:%S") {
        return Ok(Utc.from_utc_datetime(&naive_dt));
    }

    // Try parsing pure date formats and convert to start of day
    let date_formats = [
        "%Y-%m-%d", // 2022-01-01
        "%Y/%m/%d", // 2022/01/01
        "%Y.%m.%d", // 2022.01.01
        "%Y%m%d",   // 20220101 - must be exactly 8 digits
    ];

    for format in &date_formats {
        if let Ok(date) = NaiveDate::parse_from_str(ts_str, format) {
            // Additional validation for compact format to ensure it's actually a date
            if format == &"%Y%m%d" && ts_str.len() != 8 {
                continue;
            }
            // Convert to start of day in UTC
            if let Some(naive_datetime) = date.and_hms_opt(0, 0, 0) {
                return Ok(Utc.from_utc_datetime(&naive_datetime));
            }
        }
    }

    // Finally, try parsing as Unix timestamp (only if it's reasonable length and all digits)
    if ts_str.len() >= 9 && ts_str.len() <= 13 && ts_str.chars().all(|c| c.is_ascii_digit()) {
        if let Ok(timestamp) = ts_str.parse::<i64>() {
            if let Some(dt) = Utc.timestamp_opt(timestamp, 0).single() {
                return Ok(dt);
            }
        }
    }

    Err(BrokerError::ConfigurationError(format!(
        "Invalid timestamp format '{ts_str}'. Supported formats:\n\
            - Unix timestamp: '1640995200'\n\
            - RFC3339 with timezone: '2022-01-01T00:00:00+00:00', '2022-01-01T00:00:00Z', '2022-01-01T05:00:00-05:00'\n\
            - RFC3339 without timezone: '2022-01-01T00:00:00' (assumes UTC)\n\
            - Date with time: '2022-01-01 00:00:00'\n\
            - Pure date: '2022-01-01', '2022/01/01', '2022.01.01', '20220101'"
    )))
}

impl BgpStreamConfig {
    /// Creates a new stream configuration with validation.
    ///
    /// This constructor parses timestamp strings (supporting multiple formats),
    /// validates that the specified collectors exist, and returns a configuration
    /// ready for use with `BgpStream`.
    ///
    /// # Arguments
    ///
    /// * `ts_start` - Start timestamp (RFC3339 or Unix timestamp)
    /// * `ts_end` - End timestamp (RFC3339 or Unix timestamp)
    /// * `collectors` - List of collector IDs to include (e.g., "route-views.wide")
    /// * `data_type` - Type of BGP data: Update, Rib, or Both
    ///
    /// # Returns
    ///
    /// A configured `BgpStreamConfig` on success, or a `BrokerError` if:
    /// - Timestamps cannot be parsed
    /// - All collectors are invalid
    ///
    /// # Example
    ///
    /// ```no_run
    /// use bgpflux::{BgpStreamConfig, DataType};
    ///
    /// let config = BgpStreamConfig::new(
    ///     "2023-01-01T00:00:00Z",
    ///     "2023-01-01T01:00:00Z",
    ///     &["route-views.wide", "route-views.sydney"],
    ///     DataType::Update,
    /// ).expect("Failed to create config");
    /// ```
    pub fn new<S: Display>(
        ts_start: S,
        ts_end: S,
        collectors: &[&str],
        data_type: DataType,
    ) -> Result<BgpStreamConfig, BrokerError> {
        let ts_start = parse_timestamp(&ts_start.to_string())?;
        let ts_end = parse_timestamp(&ts_end.to_string())?;

        let ts_start = ts_start.format("%Y-%m-%dT%H:%M:%SZ").to_string();
        let ts_end = ts_end.format("%Y-%m-%dT%H:%M:%SZ").to_string();

        let collectors = validate_collectors(collectors)?;

        let config = BgpStreamConfig {
            ts_start,
            ts_end,
            collectors,
            data_type,
            filters: None,
        };
        Ok(config)
    }

    /// Sets the filters for the BGP stream.
    pub fn with_filters(mut self, filters: Vec<Filter>) -> Self {
        self.filters = Some(filters);
        self
    }

    /// Add a single filter to the existing list
    pub fn add_filter(mut self, filter: Filter) -> Self {
        let mut filters = self.filters.unwrap_or_default();
        filters.push(filter);
        self.filters = Some(filters);
        self
    }
}

/// Filter out non-existant collectors
pub fn validate_collectors(collectors: &[&str]) -> Result<Vec<String>, BrokerError> {
    let collectors: Vec<String> = collectors.iter().map(|e| e.to_string()).collect();

    let available_collectors: Vec<String> = load_collectors()?.into_iter().map(|c| c.id).collect();
    let (collectors, invalid_collectors): (Vec<String>, Vec<String>) = collectors
        .into_iter()
        .partition(|c| available_collectors.contains(c));
    for c in invalid_collectors {
        eprintln!("Dropped collector {} (not valid)", c);
    }
    Ok(collectors)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_config() -> Result<(), BrokerError> {
        let config = BgpStreamConfig::new(
            "1283299200",
            "1283306400",
            &["route-views.wide", "route-views.sydney", "cute cat"],
            DataType::Rib,
        )?;
        assert_eq!(
            vec![
                String::from("route-views.wide"),
                String::from("route-views.sydney")
            ],
            config.collectors
        );
        assert_eq!("2010-09-01T00:00:00Z", config.ts_start);
        assert_eq!("2010-09-01T02:00:00Z", config.ts_end);
        Ok(())
    }
}
