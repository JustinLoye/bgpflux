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
mod parser_utils;
mod runtime;
mod utils;

use bgpkit_broker::BgpkitBroker;
use chrono::DateTime;
pub use config::{BgpStreamConfig, DataType};
pub use elem::{BgpStreamElem, BgpStreamElemType};
use itertools::Either;
use itertools::Itertools;
use parser_utils::{init_parser_retry, process_parser_to_elems};
use runtime::{download_semaphore, global_runtime, intern_collector};
use std::sync::Arc;
use std::{collections::HashMap, fmt::Display};

/// Makes compiler happy when branching in `build`
enum BgpStreamIter<I1, I2> {
    WithCache(I1),
    NoCache(I2),
}
impl<I1, I2> Iterator for BgpStreamIter<I1, I2>
where
    I1: Iterator<Item = BgpStreamElem>,
    I2: Iterator<Item = BgpStreamElem>,
{
    type Item = BgpStreamElem;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::WithCache(i) => i.next(),
            Self::NoCache(i) => i.next(),
        }
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
        match self.cache_dir {
            Some(_) => BgpStreamIter::WithCache(self.build_with_cache()),
            None => BgpStreamIter::NoCache(self.build_no_cache()),
        }
    }

    /// Query the broker to return time-ordered archives for each (collectors, data type)
    fn query_broker(&self) -> HashMap<(String, bool), Vec<String>> {
        let data_types = match self.config.data_type {
            DataType::Rib => vec!["rib"],
            DataType::Update => vec!["update"],
            DataType::Both => vec!["rib", "update"],
        };

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
        grouped_urls
    }

    fn build_no_cache(self) -> impl Iterator<Item = BgpStreamElem> {
        // Collect sorted urls for each (collector, data_type) pair
        let grouped_urls = self.query_broker();

        // Chain archive files for each (collector, data_type) pair
        let start = DateTime::parse_from_rfc3339(&self.config.ts_start)
            .unwrap()
            .timestamp() as f64;
        let end = DateTime::parse_from_rfc3339(&self.config.ts_end)
            .unwrap()
            .timestamp() as f64;
        let mut streams = Vec::new();
        for ((collector, is_rib), urls) in grouped_urls.into_iter() {
            let static_collector = intern_collector(collector);

            let stream = urls.into_iter().flat_map(move |url| {
                match init_parser_retry(&url, None) {
                    Ok(parser) => Either::Left(process_parser_to_elems(
                        parser,
                        url,
                        is_rib,
                        static_collector,
                        start,
                        end,
                    )),
                    Err(e) => {
                        eprintln!("FAILED to initialize parser: {:?}", e);
                        // "Empty" iterator fallback
                        Either::Right(std::iter::empty())
                    }
                }
            });
            streams.push(stream);
        }

        // Merge them in a single sorted stream
        streams
            .into_iter()
            .kmerge_by(|a, b| a.timestamp <= b.timestamp)
    }

    fn build_with_cache(self) -> impl Iterator<Item = BgpStreamElem> {
        // Collect sorted urls for each (collector, data_type) pair
        let grouped_urls = self.query_broker();

        let cache_dir = Arc::new(self.cache_dir.clone());
        let start = DateTime::parse_from_rfc3339(&self.config.ts_start)
            .unwrap()
            .timestamp() as f64;
        let end = DateTime::parse_from_rfc3339(&self.config.ts_end)
            .unwrap()
            .timestamp() as f64;

        let rt = global_runtime();
        let sem = download_semaphore();
        let mut receivers = Vec::new();

        // Spawn Prefetch Tasks
        for (key, urls) in grouped_urls {
            // Channel restrict the number of prefetch per (collector, data type). For now hardcoded to 1.
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            receivers.push((key, rx));

            let cache = Arc::clone(&cache_dir);

            rt.spawn(async move {
                for url in urls {
                    // Acquire a permit from the global pool before starting download
                    let permit = sem.acquire().await.unwrap();

                    let cache_inner = Arc::clone(&cache);
                    let url_inner = url.clone();

                    let parser_res = tokio::task::spawn_blocking(move || {
                        init_parser_retry(&url_inner, cache_inner.as_ref().as_deref())
                    })
                    .await
                    .unwrap();

                    // Release the semaphore permit immediately after the file is ready/cached
                    // so other streams can start their downloads.
                    drop(permit);

                    match parser_res {
                        Ok(parser) => {
                            if tx.send((url, parser)).await.is_err() {
                                break;
                            }
                        }
                        Err(e) => {
                            eprintln!("FAILED to initialize parser: {:?}", e);
                        }
                    }
                }
            });
        }

        // Build stream iterators consuming the prefetched BgpkitParser objects
        let mut streams = Vec::new();
        for ((collector, is_rib), mut rx) in receivers {
            let static_collector = intern_collector(collector);

            let stream = std::iter::from_fn(move || {
                // Use the global runtime's blocking_recv
                rx.blocking_recv()
            })
            .flat_map(move |(url, parser)| {
                process_parser_to_elems(parser, url, is_rib, static_collector, start, end)
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
    use std::fs;

    use super::*;

    /// Consume the stream to check if it's consistent with `config` and the number of expected BGP elements
    fn check_stream(
        stream: impl Iterator<Item = BgpStreamElem>,
        config: BgpStreamConfig,
        target_collectors_count: HashMap<(&str, DataType), u32>,
    ) {
        let target_elem_types = match config.data_type {
            DataType::Rib => HashSet::from([BgpStreamElemType::RIB]),
            DataType::Update => {
                HashSet::from([BgpStreamElemType::ANNOUNCE, BgpStreamElemType::WITHDRAW])
            }
            DataType::Both => HashSet::from([
                BgpStreamElemType::ANNOUNCE,
                BgpStreamElemType::WITHDRAW,
                BgpStreamElemType::RIB,
            ]),
        };

        // Collect stream info
        let mut seen_collectors_count = HashMap::new();
        let mut seen_elem_types = HashSet::new();
        let mut timestamps = Vec::new();
        for elem in stream {
            seen_collectors_count
                .entry((elem.collector_id, {
                    match elem.elem_type {
                        BgpStreamElemType::RIB => DataType::Rib,
                        _ => DataType::Update,
                    }
                }))
                .and_modify(|val| *val += 1)
                .or_insert(1);
            seen_elem_types.insert(elem.elem_type);
            timestamps.push(elem.timestamp);
        }

        // Check if config and stream match
        assert_eq!(seen_collectors_count, target_collectors_count);
        assert_eq!(seen_elem_types, target_elem_types);
        assert!(timestamps.is_sorted());
    }

    #[test]
    fn stream_update() {
        let config = BgpStreamConfig::new(
            "2010-09-01T00:00:00Z",
            "2010-09-01T01:55:00Z",
            vec!["route-views.wide", "route-views.sydney"],
            config::DataType::Update,
        )
        .unwrap();

        let stream = BgpStream::new(config.clone()).build();

        let target_collectors_count = HashMap::from([
            (("route-views.sydney", DataType::Update), 48287),
            (("route-views.wide", DataType::Update), 29490),
        ]);

        check_stream(stream, config, target_collectors_count);
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
        let test_cache_dir = "test_cache";

        // Test cache miss
        fs::remove_dir_all(test_cache_dir).ok();
        fs::create_dir(test_cache_dir).unwrap();
        let stream = BgpStream::new(config.clone())
            .cache_dir(test_cache_dir)
            .build();
        let target_collectors_count = HashMap::from([
            (("route-views.sydney", DataType::Update), 48287),
            (("route-views.wide", DataType::Update), 29490),
        ]);
        check_stream(stream, config.clone(), target_collectors_count.clone());

        // Test cache hit
        let stream = BgpStream::new(config.clone())
            .cache_dir(test_cache_dir)
            .build();
        check_stream(stream, config, target_collectors_count);

        fs::remove_dir_all(test_cache_dir).unwrap();
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

        let stream = BgpStream::new(config.clone()).build();

        let target_collectors_count = HashMap::from([
            (("route-views.sydney", DataType::Rib), 828937),
            (("route-views.wide", DataType::Rib), 990164),
        ]);

        check_stream(stream, config, target_collectors_count);
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

        let stream = BgpStream::new(config.clone()).build();

        let target_collectors_count = HashMap::from([
            (("route-views.sydney", DataType::Rib), 828937),
            (("route-views.wide", DataType::Rib), 990164),
            (("route-views.sydney", DataType::Update), 48287),
            (("route-views.wide", DataType::Update), 29490),
        ]);

        check_stream(stream, config, target_collectors_count);
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
        let mut count = 0;
        let stream = BgpStream::new(config).cache_dir("test_cache").build();
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
