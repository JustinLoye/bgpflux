use chrono::NaiveDateTime;
use regex::Regex;
use std::sync::LazyLock;

static TIMESTAMP_PATTERN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(\d{8}\.\d{4})\.(?:gz|bz2)").unwrap());

pub fn timestamp_from_project_url(url: &str) -> Option<i64> {
    let caps = TIMESTAMP_PATTERN.captures(url)?;
    let time_str = caps.get(1)?.as_str();
    NaiveDateTime::parse_from_str(time_str, "%Y%m%d.%H%M")
        .ok()
        .map(|t| t.and_utc().timestamp())
}
