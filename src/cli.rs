//! CLI argument parsing and configuration

use bgpflux::DataType;
use bgpkit_parser::Filter;
use clap::{Parser, ValueEnum};
use ipnet::IpNet;
use itertools::Itertools;
use std::net::IpAddr;

// #[derive(Parser, Debug, Clone)]
// #[command(author, version, about = "A CLI to stream ordered BGP elements from multiple collectors and arbitrary time ranges", long_about = None)]
// pub struct Args {
//     /// Start timestamp (e.g., "2022-01-01T00:00:00Z" or Unix timestamp)
//     #[arg(short = 'b', long, help_heading = "Required arguments")]
//     pub start: String,

//     /// End timestamp (e.g., "2022-01-01T01:00:00Z" or Unix timestamp)
//     #[arg(short = 'e', long, help_heading = "Required arguments")]
//     pub end: String,

//     /// Data type: "update", "rib" or "update,rib"
//     #[arg(
//         short = 't',
//         long,
//         value_delimiter = ',',
//         required = true,
//         hide_possible_values = true,
//         help_heading = "Required arguments"
//     )]
//     pub data_type: Vec<DataTypeArg>,

//     /// Collectors (e.g., "-c rrc00 -c rrc01" or "-c rrc00,rrc01")
//     #[arg(
//         short,
//         long,
//         value_delimiter = ',',
//         required = true,
//         help_heading = "Required arguments"
//     )]
//     pub collector: Vec<String>,

//     /// Cache directory
//     #[arg(long, help_heading = "Optional configuration")]
//     pub cache_dir: Option<String>,

//     /// Custom broker URL
//     #[arg(long, help_heading = "Optional configuration")]
//     pub broker_url: Option<String>,

//     #[clap(flatten)]
//     pub filters: Filters,
// }

#[derive(Parser, Debug, Clone)]
#[command(
    author,
    version,
    about = "A CLI to stream ordered BGP elements from multiple collectors and arbitrary time ranges"
)]
pub struct Args {
    // #[command(subcommand)]
    // pub live: Option<LiveSubcommand>,
    /// Trigger live mode
    #[arg(short, long)]
    pub live: bool,

    /// Collectors (e.g., "-c rrc00,rrc01")
    #[arg(
        short,
        long,
        value_delimiter = ',',
        required = true,
        help_heading = "Required argument"
    )]
    pub collector: Vec<String>,

    /// Start timestamp. Required unless 'live' is used.
    #[arg(
        short = 'b',
        long,
        help_heading = "Archive mode required arguments",
        required_unless_present = "live"
    )]
    pub start: Option<String>,

    /// End timestamp. Required unless 'live' is used.
    #[arg(
        short = 'e',
        long,
        help_heading = "Archive mode required arguments",
        required_unless_present = "live"
    )]
    pub end: Option<String>,

    /// Data type: "update", "rib" or "update,rib". Required unless 'live' is used.
    #[arg(
        short = 't',
        long,
        value_delimiter = ',',
        help_heading = "Archive mode required arguments",
        required_unless_present = "live"
    )]
    pub data_type: Option<Vec<DataTypeArg>>,

    /// Cache directory
    #[arg(
        long,
        help_heading = "Archive mode optional arguments",
        conflicts_with = "live"
    )]
    pub cache_dir: Option<String>,

    /// Custom broker URL
    #[arg(
        long,
        help_heading = "Archive mode optional arguments",
        conflicts_with = "live"
    )]
    pub broker_url: Option<String>,

    #[clap(flatten)]
    pub filters: Filters,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
pub enum DataTypeArg {
    #[value(alias = "updates")]
    Update,
    #[value(alias = "ribs")]
    Rib,
}

impl DataTypeArg {
    /// Collapses a collection of CLI args into a single library DataType
    pub fn to_data_type(args: &[DataTypeArg]) -> Result<DataType, &'static str> {
        let has_update = args.contains(&DataTypeArg::Update);
        let has_rib = args.contains(&DataTypeArg::Rib);

        match (has_update, has_rib) {
            (true, true) => Ok(DataType::Both),
            (true, false) => Ok(DataType::Update),
            (false, true) => Ok(DataType::Rib),
            _ => Err("Could not parse data type"),
        }
    }
}

// This struct is adapted from bgpkit-parser
// Original source: https://github.com/bgpkit/bgpkit-parser/
// Copyright (c) 2021 Mingwei Zhang
// Licensed under the MIT License
#[derive(Parser, Debug, Clone)]
#[command(next_help_heading = "Optional filters")]
pub struct Filters {
    /// Filter by origin AS Number
    #[clap(short = 'o', long)]
    pub origin_asn: Option<u32>,

    /// Generic filter expression (can be used multiple times)
    /// Format: "key=value" for positive match, "key!=value" for negative match
    /// Examples: --filter "origin_asn!=13335" --filter "peer_ip!=192.168.1.1"
    /// For multi-value filters: --filter "origin_asns!=13335,15169" excludes both ASNs
    /// Supported keys: origin_asn, origin_asns, prefix, prefixes, peer_ip, peer_ips, peer_asn, peer_asns, type, as_path, community, ip_version
    #[clap(short = 'f', long = "filter")]
    pub filters: Vec<String>,

    /// Filter by network prefix
    #[clap(short = 'p', long)]
    pub prefix: Option<IpNet>,

    /// Include super-prefix when filtering
    #[clap(short = 's', long)]
    pub include_super: bool,

    /// Include sub-prefix when filtering
    #[clap(short = 'S', long)]
    pub include_sub: bool,

    /// Filter by IPv4 only
    #[clap(short = '4', long)]
    pub ipv4_only: bool,

    /// Filter by IPv6 only
    #[clap(short = '6', long)]
    pub ipv6_only: bool,

    /// Filter by peer IP address
    #[clap(short = 'j', long)]
    pub peer_ip: Vec<IpAddr>,

    /// Filter by peer ASN
    #[clap(short = 'J', long)]
    pub peer_asn: Option<u32>,

    /// Filter by elem type: announce (a) or withdraw (w)
    #[clap(short = 'm', long)]
    pub elem_type: Option<String>,

    /// Filter by AS path regex string
    #[clap(short = 'a', long)]
    pub as_path: Option<String>,

    /// Filter by AS path regex string
    #[clap(short = 'C', long)]
    pub community: Option<String>,
}

impl Filters {
    pub fn parse(self) -> Result<Vec<Filter>, bgpkit_parser::ParserError> {
        let mut filters: Vec<Filter> = Vec::new();

        if let Some(v) = &self.as_path {
            filters.push(Filter::new("as_path", v.as_str())?);
        }
        if let Some(v) = &self.community {
            filters.push(Filter::new("community", v.as_str())?);
        }
        if let Some(v) = &self.origin_asn {
            filters.push(Filter::new("origin_asn", v.to_string().as_str())?);
        }
        if let Some(v) = self.prefix {
            let filter_type = match (self.include_super, self.include_sub) {
                (false, false) => "prefix",
                (true, false) => "prefix_super",
                (false, true) => "prefix_sub",
                (true, true) => "prefix_super_sub",
            };
            filters.push(Filter::new(filter_type, v.to_string().as_str())?);
        }
        if !self.peer_ip.is_empty() {
            let v = self.peer_ip.iter().map(|p| p.to_string()).join(",");
            filters.push(Filter::new("peer_ips", v.as_str())?);
        }
        if let Some(v) = self.peer_asn {
            filters.push(Filter::new("peer_asn", v.to_string().as_str())?);
        }
        if let Some(v) = &self.elem_type {
            filters.push(Filter::new("type", v.as_str())?);
        }

        // Process generic filter expressions
        for filter_expr in &self.filters {
            match parse_filter_expression(filter_expr) {
                Ok((filter_type, filter_value)) => match Filter::new(&filter_type, &filter_value) {
                    Ok(f) => filters.push(f),
                    Err(e) => {
                        eprintln!("Error adding filter '{}': {}", filter_expr, e);
                        std::process::exit(1);
                    }
                },
                Err(e) => {
                    eprintln!("Invalid filter expression '{}': {}", filter_expr, e);
                    std::process::exit(1);
                }
            }
        }

        match (self.ipv4_only, self.ipv6_only) {
            (true, true) => {
                eprintln!("Error: --ipv4-only and --ipv6-only cannot be used together");
                std::process::exit(1);
            }
            (false, false) => {
                // no filters on IP version, skip
            }
            (true, false) => {
                filters.push(Filter::new("ip_version", "ipv4")?);
            }
            (false, true) => {
                filters.push(Filter::new("ip_version", "ipv6")?);
            }
        }

        Ok(filters)
    }
}

// This function is copied from bgpkit-parser
// Original source: https://github.com/bgpkit/bgpkit-parser/
// Copyright (c) 2021 Mingwei Zhang
// Licensed under the MIT License
/// Parse a filter expression in the format "key=value" or "key!=value"
/// Returns (filter_type, filter_value) where filter_value may be prefixed with "!" for negation
///
/// For multi-value filters (e.g., "origin_asns!=13335,15169"), the negation is distributed
/// to each value: ("origin_asns", "!13335,!15169")
pub fn parse_filter_expression(expr: &str) -> Result<(String, String), String> {
    // Multi-value filter types that support comma-separated values
    let multi_value_filters = [
        "origin_asns",
        "prefixes",
        "prefixes_super",
        "prefixes_sub",
        "prefixes_super_sub",
        "peer_ips",
        "peer_asns",
    ];

    // Check for "!=" (negative filter) first
    if let Some(pos) = expr.find("!=") {
        let key = expr[..pos].trim();
        let value = expr[pos + 2..].trim();
        if key.is_empty() {
            return Err("filter key cannot be empty".to_string());
        }
        if value.is_empty() {
            return Err("filter value cannot be empty".to_string());
        }

        // For multi-value filters, prefix each value with "!"
        if multi_value_filters.contains(&key) {
            let negated_values: Vec<String> =
                value.split(',').map(|v| format!("!{}", v.trim())).collect();
            Ok((key.to_string(), negated_values.join(",")))
        } else {
            // For single-value filters, prefix the value with "!"
            Ok((key.to_string(), format!("!{}", value)))
        }
    }
    // Check for "=" (positive filter)
    else if let Some(pos) = expr.find('=') {
        let key = expr[..pos].trim();
        let value = expr[pos + 1..].trim();
        if key.is_empty() {
            return Err("filter key cannot be empty".to_string());
        }
        if value.is_empty() {
            return Err("filter value cannot be empty".to_string());
        }
        Ok((key.to_string(), value.to_string()))
    } else {
        Err("filter expression must contain '=' or '!=' (e.g., 'origin_asn=13335' or 'origin_asn!=13335')".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_type_arg_single_update() {
        let args = vec![DataTypeArg::Update];
        let result = DataTypeArg::to_data_type(&args);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), DataType::Update);
    }

    #[test]
    fn test_data_type_arg_single_rib() {
        let args = vec![DataTypeArg::Rib];
        let result = DataTypeArg::to_data_type(&args);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), DataType::Rib);
    }

    #[test]
    fn test_data_type_arg_both() {
        let args = vec![DataTypeArg::Update, DataTypeArg::Rib];
        let result = DataTypeArg::to_data_type(&args);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), DataType::Both);
    }

    #[test]
    fn test_data_type_arg_empty() {
        let args: Vec<DataTypeArg> = vec![];
        let result = DataTypeArg::to_data_type(&args);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_filter_expression_single_value_positive() {
        let result = parse_filter_expression("origin_asn=13335");
        assert!(result.is_ok());
        let (key, value) = result.unwrap();
        assert_eq!(key, "origin_asn");
        assert_eq!(value, "13335");
    }

    #[test]
    fn test_parse_filter_expression_multi_value_negative() {
        let result = parse_filter_expression("origin_asns!=13335,15169");
        assert!(result.is_ok());
        let (key, value) = result.unwrap();
        assert_eq!(key, "origin_asns");
        assert_eq!(value, "!13335,!15169");
    }

    #[test]
    fn test_parse_filter_expression_invalid_no_operator() {
        let result = parse_filter_expression("origin_asn13335");
        assert!(result.is_err());
    }
}
