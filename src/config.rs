use std::fmt::Display;

use bgpkit_broker::{BrokerError, load_collectors};
use chrono::{DateTime, NaiveDate, TimeZone, Utc};

#[derive(Debug)]
pub enum DataType {
    Update,
    Rib,
}

#[derive(Debug)]
pub struct BgpStreamConfig {
    pub ts_start: String,
    pub ts_end: String,
    pub collectors: Vec<String>,
    pub data_type: DataType,
}

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
    pub fn new<S: Display>(
        ts_start: S,
        ts_end: S,
        collectors: impl IntoIterator<Item = impl Into<String>>,
        data_type: DataType,
    ) -> Result<BgpStreamConfig, BrokerError> {
        let ts_start = parse_timestamp(&ts_start.to_string())?;
        let ts_end = parse_timestamp(&ts_end.to_string())?;

        let ts_start = ts_start.format("%Y-%m-%dT%H:%M:%SZ").to_string();
        let ts_end = ts_end.format("%Y-%m-%dT%H:%M:%SZ").to_string();

        let collectors: Vec<String> = collectors.into_iter().map(|c| c.into()).collect();

        let available_collectors: Vec<String> =
            load_collectors()?.into_iter().map(|c| c.id).collect();
        let (collectors, invalid_collectors): (Vec<String>, Vec<String>) = collectors
            .into_iter()
            .partition(|c| available_collectors.contains(c));
        for c in invalid_collectors {
            eprintln!("Dropped collector {} (not valid)", c);
        }

        let config = BgpStreamConfig {
            ts_start,
            ts_end,
            collectors,
            data_type,
        };
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_config() -> Result<(), BrokerError> {
        let config = BgpStreamConfig::new(
            "1283299200",
            "1283306400",
            vec!["route-views.wide", "route-views.sydney", "cute cat"],
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
