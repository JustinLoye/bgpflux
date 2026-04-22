use crate::elem::BgpStreamElem;
use crate::runtime::intern_collector;
use bgpkit_parser::bmp::messages::BmpMessageBody;
use bgpkit_parser::Elementor;
use bgpkit_parser::{parse_bmp_msg, parse_openbmp_header};
use bytes::Bytes;
use kafka::client::KafkaClient;
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use std::collections::VecDeque;
use std::thread::sleep;
use std::time::Duration;

fn build_pattern(collectors: &[String]) -> String {
    let escaped: Vec<String> = collectors.iter().map(|c| regex::escape(c)).collect();
    let alternation = escaped.join("|");
    format!(r#"^routeviews\.({})\..*\.bmp_raw$"#, alternation)
}

fn get_matching_topics(client: &mut KafkaClient, pattern: &str) -> Vec<String> {
    let re = regex::Regex::new(pattern).expect("Invalid regex pattern");
    client.load_metadata_all().expect("Failed to load metadata");
    client
        .topics()
        .iter()
        .filter(|t| re.is_match(t.name())) // Adjust pattern matching as necessary
        .map(|t| t.name().to_string())
        .collect()
}
pub struct RouteviewsStream {
    con: Consumer,
    buffer: VecDeque<BgpStreamElem>,
}

// This struct is adapted from bgpkit-parser
// Original source: https://github.com/bgpkit/bgpkit-parser/
// Copyright (c) 2021 Mingwei Zhang
// Licensed under the MIT License
impl RouteviewsStream {
    pub fn new(collectors: &[String]) -> Self {
        let brokers = vec!["stream.routeviews.org:9092".to_owned()];

        //r#"routeviews\.amsix\.ams\..*\.bmp_raw"#.to_owned();
        let pattern = build_pattern(collectors.as_ref());

        let group = format!("bgpflux-{}", chrono::Utc::now().timestamp());

        let mut client = KafkaClient::new(brokers);
        client
            .load_metadata_all()
            .expect("Failed to connect to Kafka");
        let topics = get_matching_topics(&mut client, pattern.as_str());
        dbg!(&topics);

        let mut builder = Consumer::from_client(client);
        for topic in topics {
            builder = builder.with_topic(topic);
        }
        let con = builder
            .with_group(group)
            .with_fetch_max_bytes_per_partition(100_000)
            .with_retry_max_bytes_limit(1_000_000)
            .with_fallback_offset(FetchOffset::Latest)
            .with_offset_storage(Some(GroupOffsetStorage::Kafka))
            .create()
            .expect("Could not establish connection to RouteViews");
        Self {
            con,
            buffer: VecDeque::new(),
        }
    }
}

impl Iterator for RouteviewsStream {
    type Item = BgpStreamElem;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(elem) = self.buffer.pop_front() {
                return Some(elem);
            }

            let mss = self.con.poll().ok()?;
            if mss.is_empty() {
                // println!("No messages available right now, wait for 5 seconds.");
                sleep(Duration::from_secs(5));
                continue;
            }

            for ms in mss.iter() {
                for m in ms.messages() {
                    let mut bytes = Bytes::from(m.value.to_vec());
                    let header = parse_openbmp_header(&mut bytes).unwrap();
                    let collector = intern_collector(header.router_group.unwrap());
                    let bmp_msg = parse_bmp_msg(&mut bytes);
                    match bmp_msg {
                        Ok(msg) => {
                            let per_peer_header = msg.per_peer_header.unwrap();
                            if let BmpMessageBody::RouteMonitoring(m) = msg.message_body {
                                let timestamp = if per_peer_header.timestamp > 0.0 {
                                    per_peer_header.timestamp
                                } else {
                                    header.timestamp
                                };

                                for elem in Elementor::bgp_to_elems(
                                    m.bgp_message,
                                    timestamp,
                                    &per_peer_header.peer_ip,
                                    &per_peer_header.peer_asn,
                                ) {
                                    let bgpstreamelem = BgpStreamElem {
                                        collector_id: collector,
                                        elem_type: elem.elem_type.into(),
                                        elem,
                                    };
                                    self.buffer.push_back(bgpstreamelem);
                                }
                            }
                        }
                        Err(_e) => {
                            continue;
                        }
                    }
                }
                let _ = self.con.consume_messageset(ms);
            }
            self.con.commit_consumed().ok()?;
        }
    }
}
