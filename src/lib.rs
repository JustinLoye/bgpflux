pub mod config;
pub mod elem;
pub mod utils;

use std::{collections::HashMap, fmt::Display};

use bgpkit_broker::BgpkitBroker;
use bgpkit_parser::BgpkitParser;
pub use config::{BgpStreamConfig, DataType};
pub use elem::{BgpStreamElem, BgpStreamElemType};
use itertools::Itertools;
use utils::timestamp_from_project_url;

pub struct BgpStream {
    pub config: BgpStreamConfig,
    pub broker_url: Option<String>,
    pub cache_dir: Option<String>,
}

impl BgpStream {
    pub fn new(config: BgpStreamConfig) -> Self {
        BgpStream {
            config,
            broker_url: None,
            cache_dir: None,
        }
    }

    pub fn broker_url<S: Display>(mut self, broker_url: S) -> Self {
        self.broker_url = Some(broker_url.to_string());
        self
    }

    pub fn cache_dir<S: Display>(mut self, cache_dir: S) -> Self {
        self.cache_dir = Some(cache_dir.to_string());
        self
    }

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

        let mut streams = Vec::new();
        for ((collector, is_rib), urls) in grouped_urls.into_iter() {
            let static_collector: &'static str = Box::leak(collector.into_boxed_str());

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

                parser.unwrap().into_iter().map(move |mut elem| {
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
            "2010-09-01T02:00:00Z",
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
                .or_insert(0);
            seen_elem_types.insert(elem.elem_type);
            timestamps.push(elem.timestamp);
            count += 1;

            if count < 20 {
                println!("{}", elem);
            }
        }

        assert_eq!(count, 81783);
        assert_eq!(collectors_count["route-views.sydney"], 50551);
        assert_eq!(collectors_count["route-views.wide"], 31230);
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
            "2010-09-01T02:00:00Z",
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
                .or_insert(0);
            seen_elem_types.insert(elem.elem_type);
            timestamps.push(elem.timestamp);
            count += 1;

            if count < 20 {
                println!("{}", elem);
            }
        }

        assert_eq!(count, 81783);
        assert_eq!(collectors_count["route-views.sydney"], 50551);
        assert_eq!(collectors_count["route-views.wide"], 31230);
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
                .or_insert(0);
            seen_elem_types.insert(elem.elem_type);
            timestamps.push(elem.timestamp);
            count += 1;

            if count < 20 {
                println!("{}", elem);
            }
        }

        assert_eq!(count, 1819101);
        assert_eq!(collectors_count["route-views.sydney"], 828936);
        assert_eq!(collectors_count["route-views.wide"], 990163);
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
                .or_insert(0);
            seen_elem_types.insert(elem.elem_type);
            timestamps.push(elem.timestamp);
            count += 1;

            if count < 20 {
                println!("{}", elem);
            }
        }
        // println!({})
        assert_eq!(count, 1819101 + 81783);
        assert_eq!(collectors_count[&("route-views.sydney", "rib")], 828936);
        assert_eq!(collectors_count[&("route-views.wide", "rib")], 990163);
        assert_eq!(collectors_count[&("route-views.sydney", "update")], 50551);
        assert_eq!(collectors_count[&("route-views.wide", "update")], 31230);
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
