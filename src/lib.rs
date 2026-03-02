mod config;
mod elem;

use std::{collections::HashMap, fmt::Display};

use bgpkit_broker::{BgpkitBroker, BrokerItem};
use bgpkit_parser::BgpkitParser;
use config::BgpStreamConfig;
use elem::BgpStreamElem;
use itertools::Itertools;

pub struct BgpStream {
    pub config: BgpStreamConfig,
    pub broker: BgpkitBroker,
}

impl BgpStream {
    fn new(config: BgpStreamConfig) -> Self {
        let broker = BgpkitBroker::new()
            .ts_start(config.ts_start.clone())
            .ts_end(config.ts_end.clone())
            .collector_id(config.collectors.join(","))
            .data_type("update");

        BgpStream {
            config,
            broker,
            // iterator: None,
        }
    }

    fn broker_url<S: Display>(mut self, broker_url: S) -> Self {
        self.broker = self.broker.broker_url(broker_url);
        self
    }

    // Call the broker and set up the iterator
    fn build(mut self) -> impl Iterator<Item = BgpStreamElem> {
        let broker = std::mem::replace(&mut self.broker, BgpkitBroker::new());
        let items: Vec<BrokerItem> = broker.into_iter().collect();
        let mut collector_urls: HashMap<String, Vec<String>> = HashMap::new();
        for item in &items {
            collector_urls
                .entry(item.collector_id.clone())
                .or_insert_with(Vec::new)
                .push(item.url.clone());
        }
        println!("{:?}", collector_urls);

        collector_urls
            .into_iter()
            .map(|(collector, urls)| {
                // This happens ONLY ONCE per collector, not per element.
                let static_collector: &'static str = Box::leak(collector.into_boxed_str());

                urls.into_iter().flat_map(move |url| {
                    BgpkitParser::new_cached(&url, "../pybgpkitstream/cache/")
                        .unwrap()
                        .into_iter()
                        .map(move |elem| BgpStreamElem {
                            collector_id: static_collector,
                            elem,
                        })
                })
            })
            .kmerge_by(|a, b| a.timestamp <= b.timestamp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_broker_url() {
        let config = BgpStreamConfig::new(
            "2010-09-01T00:00:00Z",
            "2010-09-01T02:00:00Z",
            vec!["route-views.amsix"],
            config::DataType::Update,
        )
        .unwrap();

        let bgpstream = BgpStream::new(config);
        assert_eq!(
            bgpstream.broker.broker_url,
            "https://api.bgpkit.com/v3/broker"
        );
    }

    #[test]
    fn custom_broker_url() {
        let config = BgpStreamConfig::new(
            "2010-09-01T00:00:00Z",
            "2010-09-01T02:00:00Z",
            vec!["route-views.amsix"],
            config::DataType::Update,
        )
        .unwrap();

        let bgpstream = BgpStream::new(config).broker_url("http://example.com");
        assert_eq!(bgpstream.broker.broker_url, "http://example.com");
    }

    #[test]
    fn stream() {
        let config = BgpStreamConfig::new(
            "2010-09-01T00:00:00Z",
            "2010-09-01T02:00:00Z",
            vec!["route-views.wide", "route-views.sydney"],
            config::DataType::Update,
        )
        .unwrap();

        let stream = BgpStream::new(config).build();
        let mut count: u32 = 0;
        let mut collectors_count: HashMap<_, u32> = HashMap::new();
        for elem in stream {
            collectors_count
                .entry(elem.collector_id)
                .and_modify(|val| *val += 1)
                .or_insert(0);
            count += 1;
        }
        println!("count is {}", count);
        assert_eq!(count, 81783);
        println!("collectors count {:?}", collectors_count)
    }

    #[test]
    fn bench_throughput() {
        let config = BgpStreamConfig::new(
            "2015-09-01T00:00:00Z",
            "2015-09-01T05:00:00Z",
            vec!["route-views.wide", "route-views.sydney"],
            config::DataType::Update,
        )
        .unwrap();

        let start = std::time::Instant::now();
        let count = BgpStream::new(config).build().count();
        let elapsed = start.elapsed();

        let throughput = count as f64 / elapsed.as_secs_f64();
        println!(
            "{} elements in {:.2?} ({:.0} elem/sec)",
            count, elapsed, throughput
        );
    }
}
