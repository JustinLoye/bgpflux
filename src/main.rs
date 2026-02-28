use bgpkit_broker::{BgpkitBroker, BrokerItem};
use bgpkit_parser::{BgpElem, BgpkitParser};
use itertools::Itertools;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub struct BgpStreamElem {
    pub collector_id: String,
    pub elem: BgpElem,
}

impl std::ops::Deref for BgpStreamElem {
    type Target = BgpElem;
    fn deref(&self) -> &Self::Target {
        &self.elem
    }
}

fn main() {
    let collectors = "route-views.wide,route-views.sydney,jojolasticot";
    let ts_start = "2010-09-01T00:00:00Z";
    let ts_end = "2010-09-01T02:00:00Z";
    let data_type = "rib";

    let broker = BgpkitBroker::new()
        .collector_id(collectors)
        .ts_start(ts_start)
        .ts_end(ts_end)
        .data_type(data_type);

    let items: Vec<BrokerItem> = broker.into_iter().collect();

    let mut collector_urls: HashMap<&str, Vec<&str>> = HashMap::new();
    for item in &items {
        collector_urls
            .entry(item.collector_id.as_str())
            .or_insert_with(Vec::new)
            .push(item.url.as_str());
    }

    for (k, v) in &collector_urls {
        println!("{k}");
        for url in v {
            println!("{url}");
        }
    }

    let mut elems: Vec<BgpElem> = Vec::new();
    for elem in collector_urls
        .values()
        .map(|urls| {
            urls.into_iter()
                .flat_map(|url| BgpkitParser::new_cached(url, "../pybgpkitstream/cache/").unwrap())
        })
        .kmerge()
    {
        elems.push(elem);
    }

    println!("{}", elems[0]);

    match elems.iter().is_sorted_by(|a, b| a.timestamp <= b.timestamp) {
        true => {
            println!("All {} BGP elems are sorted", elems.len())
        }
        false => {
            println!("BGP elems are not sorted :(")
        }
    };
}
