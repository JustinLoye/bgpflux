//! # bgpflux
//!
//! A Rust library and CLI tool for streaming ordered BGP elements from multiple route collectors
//! with support for both RIB and update data types.
//!
//! ## Features
//!
//! - **Streaming Architecture**: Efficiently process BGP data without loading everything into memory
//! - **Multi-Collector Support**: Aggregate BGP updates from multiple route collectors
//! - **Sorted Output**: All BGP elements are automatically merged in chronological order across collectors
//! - **Caching**: Optional local file caching to avoid re-downloading data
//! - **Flexible Data Types**: Support for both RIB dumps and Update messages
//! - **Customizable Filtering**: Filter by collectors, time ranges, and data types
//! - **High Performance**: Built with Rust for maximum speed and memory efficiency
//!
//! ## Quick Start
//!
//! ```no_run
//! use bgpflux::{BgpStream, BgpStreamConfig, DataType};
//!
//! let config = BgpStreamConfig::new(
//!     "2010-09-01T00:00:00Z",
//!     "2010-09-01T01:00:00Z",
//!     vec!["route-views.wide", "route-views.sydney"],
//!     DataType::Update,
//! )?;
//!
//! let stream = BgpStream::new(config).build();
//!
//! for elem in stream {
//!     println!("{}", elem);
//! }
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! ## Core Components
//!
//! - **[`BgpStream`]**: Main streaming interface that aggregates data from multiple collectors
//! - **[`BgpStreamConfig`]**: Configuration for time ranges, collectors, and data types
//! - **[`BgpStreamElem`]**: Represents a single BGP element with metadata
//!
//! ## Acknowledgments
//!
//! This project uses code adapted from:
//! - [bgpkit-broker](https://github.com/bgpkit/bgpkit-broker) for timestamp parsing
//! - [bgpkit-parser](https://github.com/bgpkit/bgpkit-parser) for BGP data parsing

pub mod config;
pub mod elem;
pub mod utils;

use std::{collections::HashMap, fmt::Display};

use bgpkit_broker::BgpkitBroker;
use bgpkit_parser::BgpkitParser;
use chrono::DateTime;
pub use config::{BgpStreamConfig, DataType};
pub use elem::{BgpStreamElem, BgpStreamElemType};
use itertools::Itertools;
use utils::timestamp_from_project_url;

use std::collections::HashSet;
use std::sync::{LazyLock, Mutex};

static INTERNED_COLLECTORS: LazyLock<Mutex<HashSet<&'static str>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

fn intern_collector(s: String) -> &'static str {
    let mut cache = INTERNED_COLLECTORS.lock().unwrap();
    if let Some(&existing) = cache.get(s.as_str()) {
        existing
    } else {
        let leaked: &'static str = Box::leak(s.into_boxed_str());
        cache.insert(leaked);
        leaked
    }
}

/// The main streaming interface for BGP elements from multiple collectors.
///
/// # Examples
///
/// Basic streaming from multiple collectors:
///
/// ```no_run
/// use bgpflux::{BgpStream, BgpStreamConfig, DataType};
///
/// let config = BgpStreamConfig::new(
///     "2010-09-01T00:00:00Z",
///     "2010-09-01T01:00:00Z",
///     vec!["route-views.wide", "route-views.sydney"],
///     DataType::Update,
/// ).unwrap();
///
/// let stream = BgpStream::new(config).build();
///
/// for elem in stream {
///     println!("{}", elem);
/// }
/// ```
///
/// With caching:
///
/// ```no_run
/// use bgpflux::{BgpStream, BgpStreamConfig, DataType};
///
/// let config = BgpStreamConfig::new(
///     "2023-01-01T00:00:00Z",
///     "2023-01-01T01:00:00Z",
///     vec!["route-views.wide"],
///     DataType::Update,
/// ).unwrap();
///
/// let stream = BgpStream::new(config)
///     .cache_dir("./bgp_cache")
///     .build();
///
/// for elem in stream {
///     println!("{}", elem);
/// }
/// ```
pub struct BgpStream {
    /// Configuration for time ranges, collectors, and data types
    pub config: BgpStreamConfig,
    /// Optional custom broker URL
    pub broker_url: Option<String>,
    /// Optional local cache directory
    pub cache_dir: Option<String>,
}

impl BgpStream {
    /// Creates a new stream with the given configuration.
    pub fn new(config: BgpStreamConfig) -> Self {
        BgpStream {
            config,
            broker_url: None,
            cache_dir: None,
        }
    }

    /// Sets a custom broker URL for discovering BGP archives.
    pub fn broker_url<S: Display>(mut self, broker_url: S) -> Self {
        self.broker_url = Some(broker_url.to_string());
        self
    }

    /// Sets a local cache directory for downloaded files.
    pub fn cache_dir<S: Display>(mut self, cache_dir: S) -> Self {
        self.cache_dir = Some(cache_dir.to_string());
        self
    }

    /// Builds the stream and returns an iterator over BGP elements ordered by timestamp.
    pub fn build(self) -> impl Iterator<Item = BgpStreamElem> {
        let data_types = match self.config.data_type {
            DataType::Rib => vec!["rib"],
            DataType::Update => vec!["update"],
            DataType::Both => vec!["rib", "update"],
        };

        // Collect sorted urls for each (collector, data_type) pair
        let mut grouped_urls = HashMap::new();
        for data_type in data_types {
            let broker = BgpkitBroker::new()
                .ts_start(self.config.ts_start.clone())
                .ts_end(self.config.ts_end.clone())
                .collector_id(self.config.collectors.join(","))
                .data_type(data_type);
            let broker = match &self.broker_url {
                Some(url) => broker.broker_url(url),
                None => broker,
            };

            let is_rib = data_type == "rib";
            for item in broker.into_iter() {
                grouped_urls
                    .entry((item.collector_id, is_rib))
                    .or_insert_with(Vec::new)
                    .push(item.url.clone())
            }
        }

        // Chain archive files for each (collector, data_type) pair
        let cache_dir = self.cache_dir.clone();
        let start = DateTime::parse_from_rfc3339(&self.config.ts_start)
            .unwrap()
            .timestamp() as f64;
        let end = DateTime::parse_from_rfc3339(&self.config.ts_end)
            .unwrap()
            .timestamp() as f64;

        let mut streams = Vec::new();
        for ((collector, is_rib), urls) in grouped_urls.into_iter() {
            // let static_collector: &'static str = Box::leak(collector.into_boxed_str());
            let static_collector = intern_collector(collector);

            let cache_dir = cache_dir.clone();

            let stream = urls.into_iter().flat_map(move |url| {
                let rib_timestamp: Option<f64> = if is_rib {
                    Some(timestamp_from_project_url(&url).unwrap() as f64)
                } else {
                    None
                };

                let parser = match &cache_dir {
                    Some(path) => BgpkitParser::new_cached(&url, path),
                    None => BgpkitParser::new(&url),
                };
                parser
                    .unwrap()
                    .into_iter()
                    .take_while(move |elem| elem.timestamp <= end)
                    .map(move |mut elem| {
                        let stream_type = if is_rib {
                            elem.timestamp =
                                rib_timestamp.expect("expected rib timestamp for rib file");
                            BgpStreamElemType::RIB
                        } else {
                            elem.elem_type.into()
                        };
                        BgpStreamElem {
                            collector_id: static_collector,
                            elem_type: stream_type,
                            elem,
                        }
                    })
                    .skip_while(move |elem| elem.timestamp < start - 0.1)
            });
            streams.push(stream);
        }

        // Merge them in a single sorted stream
        streams
            .into_iter()
            .kmerge_by(|a, b| a.timestamp <= b.timestamp)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn stream_update() {
        let config = BgpStreamConfig::new(
            "2010-09-01T00:00:00Z",
            "2010-09-01T01:55:00Z",
            vec!["route-views.wide", "route-views.sydney"],
            config::DataType::Update,
        )
        .unwrap();

        let stream = BgpStream::new(config).build();
        let mut count: u32 = 0;
        let mut collectors_count = HashMap::new();
        let mut seen_elem_types = HashSet::new();
        let mut timestamps = Vec::new();
        for elem in stream {
            collectors_count
                .entry(elem.collector_id)
                .and_modify(|val| *val += 1)
                .or_insert(1);
            seen_elem_types.insert(elem.elem_type);
            timestamps.push(elem.timestamp);
            count += 1;

            if count < 20 {
                println!("{}", elem);
            }
        }
        assert_eq!(count, 48287 + 29490);
        assert_eq!(collectors_count["route-views.sydney"], 48287);
        assert_eq!(collectors_count["route-views.wide"], 29490);
        assert_eq!(
            seen_elem_types,
            HashSet::from([BgpStreamElemType::ANNOUNCE, BgpStreamElemType::WITHDRAW])
        );
        assert!(timestamps.is_sorted());
    }

    #[test]
    fn stream_cache() {
        let config = BgpStreamConfig::new(
            "2010-09-01T00:00:00Z",
            "2010-09-01T01:55:00Z",
            vec!["route-views.wide", "route-views.sydney"],
            config::DataType::Update,
        )
        .unwrap();

        let stream = BgpStream::new(config).cache_dir("cache").build();
        let mut count: u32 = 0;
        let mut collectors_count = HashMap::new();
        let mut seen_elem_types = HashSet::new();
        let mut timestamps = Vec::new();
        for elem in stream {
            collectors_count
                .entry(elem.collector_id)
                .and_modify(|val| *val += 1)
                .or_insert(1);
            seen_elem_types.insert(elem.elem_type);
            timestamps.push(elem.timestamp);
            count += 1;

            if count < 20 {
                println!("{}", elem);
            }
        }

        assert_eq!(count, 48287 + 29490);
        assert_eq!(collectors_count["route-views.sydney"], 48287);
        assert_eq!(collectors_count["route-views.wide"], 29490);
        assert_eq!(
            seen_elem_types,
            HashSet::from([BgpStreamElemType::ANNOUNCE, BgpStreamElemType::WITHDRAW])
        );
        assert!(timestamps.is_sorted());
    }

    #[test]
    fn stream_rib() {
        let config = BgpStreamConfig::new(
            "2010-09-01T00:00:00Z",
            "2010-09-01T1:55:00Z",
            vec!["route-views.wide", "route-views.sydney"],
            config::DataType::Rib,
        )
        .unwrap();

        let stream = BgpStream::new(config).build();
        let mut count: u32 = 0;
        let mut collectors_count = HashMap::new();
        let mut seen_elem_types = HashSet::new();
        let mut timestamps = Vec::new();
        for elem in stream {
            collectors_count
                .entry(elem.collector_id)
                .and_modify(|val| *val += 1)
                .or_insert(1);
            seen_elem_types.insert(elem.elem_type);
            timestamps.push(elem.timestamp);
            count += 1;

            if count < 20 {
                println!("{}", elem);
            }
        }
        println!("{:?}", collectors_count);
        assert_eq!(count, 828937 + 990164);
        assert_eq!(collectors_count["route-views.sydney"], 828937);
        assert_eq!(collectors_count["route-views.wide"], 990164);
        assert_eq!(seen_elem_types, HashSet::from([BgpStreamElemType::RIB]));
        assert!(timestamps.is_sorted());
    }

    #[test]
    fn stream_both() {
        let config = BgpStreamConfig::new(
            "2010-09-01T00:00:00Z",
            "2010-09-01T1:55:00Z",
            vec!["route-views.wide", "route-views.sydney"],
            config::DataType::Both,
        )
        .unwrap();

        let stream = BgpStream::new(config).build();
        let mut count: u32 = 0;
        let mut collectors_count = HashMap::new();
        let mut seen_elem_types = HashSet::new();
        let mut timestamps = Vec::new();
        for elem in stream {
            collectors_count
                .entry((elem.collector_id, {
                    match elem.elem_type {
                        BgpStreamElemType::RIB => "rib",
                        _ => "update",
                    }
                }))
                .and_modify(|val| *val += 1)
                .or_insert(1);
            seen_elem_types.insert(elem.elem_type);
            timestamps.push(elem.timestamp);
            count += 1;

            if count < 20 {
                println!("{}", elem);
            }
        }
        assert_eq!(count, 828937 + 990164 + 48287 + 29490);
        assert_eq!(collectors_count[&("route-views.sydney", "rib")], 828937);
        assert_eq!(collectors_count[&("route-views.wide", "rib")], 990164);
        assert_eq!(collectors_count[&("route-views.sydney", "update")], 48287);
        assert_eq!(collectors_count[&("route-views.wide", "update")], 29490);
        assert_eq!(
            seen_elem_types,
            HashSet::from([
                BgpStreamElemType::RIB,
                BgpStreamElemType::ANNOUNCE,
                BgpStreamElemType::WITHDRAW
            ])
        );
        assert!(timestamps.is_sorted());
    }

    #[test]
    #[ignore]
    // Run it with cargo test bench_throughput --release -- --nocapture --ignored
    fn bench_throughput() {
        let config = BgpStreamConfig::new(
            "2026-02-04T15:59:00Z",
            "2026-02-04T18:59:00Z",
            vec!["route-views.amsix", "route-views.linx"],
            config::DataType::Update,
        )
        .unwrap();

        let start = std::time::Instant::now();
        // let count = BgpStream::new(config).build().count();
        let mut count = 0;
        let stream = BgpStream::new(config).build();
        for elem in stream {
            std::hint::black_box(&elem);
            count += 1;
        }
        let elapsed = start.elapsed();

        let throughput = count as f64 / elapsed.as_secs_f64();
        println!(
            "{} elements in {:.2?} ({:.0} elem/sec)",
            count, elapsed, throughput
        );
    }

    #[test]
    #[ignore]
    // Run it with cargo test bench_cache_throughput --release -- --nocapture --ignored
    fn bench_cache_throughput() {
        let config = BgpStreamConfig::new(
            "2026-02-04T15:59:00Z",
            "2026-02-04T18:59:00Z",
            vec!["route-views.amsix", "route-views.linx"],
            config::DataType::Update,
        )
        .unwrap();

        let start = std::time::Instant::now();
        // let count = BgpStream::new(config).build().count();
        let mut count = 0;
        let stream = BgpStream::new(config).cache_dir("cache").build();
        for elem in stream {
            std::hint::black_box(&elem);
            count += 1;
        }
        let elapsed = start.elapsed();

        let throughput = count as f64 / elapsed.as_secs_f64();
        println!(
            "{} elements in {:.2?} ({:.0} elem/sec)",
            count, elapsed, throughput
        );
    }
}
