use bgpkit_parser::{BgpElem, BgpkitParser};

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
    // let collectors = "route-views.wide,route-views.sydney,jojolasticot";
    // let ts_start = "2010-09-01T00:00:00Z";
    // let ts_end = "2010-09-01T02:00:00Z";
    // let data_type = "rib";

    // let broker = BgpkitBroker::new()
    //     .collector_id(collectors)
    //     .ts_start(ts_start)
    //     .ts_end(ts_end)
    //     .data_type(data_type);
    let parser = BgpkitParser::new_cached(
        "https://routeviews.org/route-views.wide/bgpdata/2010.09/UPDATES/updates.20100901.0000.bz2",
        "../pybgpkitstream/cache",
    )
    .unwrap();
    let mut count = 0;
    for elem in parser {
        println!("{}", elem);
        count += 1;
        if count > 5 {
            break;
        }
    }
}
