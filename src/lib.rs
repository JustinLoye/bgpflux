mod config;

use std::{collections::HashMap, fmt::Display};

use bgpkit_broker::{BgpkitBroker, BrokerItem};
use bgpkit_parser::{BgpElem, BgpkitParser};
use config::BgpStreamConfig;
use itertools::Itertools;

pub struct BgpStream {
    pub config: BgpStreamConfig,
    pub broker: BgpkitBroker,

    // Created after build call
    // collector_urls: HashMap<String, Vec<String>>,
    iterator: Option<Box<dyn Iterator<Item = BgpElem>>>,
}

struct DataTypeBrokers {
    update: Option<BgpkitBroker>, // collector_id -> urls
    rib: Option<BgpkitBroker>,    // collector_id -> urls
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
            iterator: None,
        }
    }

    fn broker_url<S: Display>(mut self, broker_url: S) -> Self {
        self.broker = self.broker.broker_url(broker_url);
        self
    }

    // Call the broker and set up the iterator
    fn build(mut self) -> Self {
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

        self.iterator = Some(Box::new(
            collector_urls
                .into_values()
                .map(|urls| {
                    urls.into_iter().flat_map(|url| {
                        BgpkitParser::new_cached(&url, "../pybgpkitstream/cache/").unwrap()
                    })
                })
                .kmerge(),
        ));
        self
    }
}

impl Iterator for BgpStream {
    type Item = BgpElem;
    fn next(&mut self) -> Option<Self::Item> {
        self.iterator.as_mut()?.next()
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

        // let stream = BgpStream::new(config).build();
        let count = BgpStream::new(config).build().count();
        // for elem in stream {
        //     println!("{}", elem)
        // }
        println!("count is {}", count);
        assert_eq!(count, 81783)
    }
}
