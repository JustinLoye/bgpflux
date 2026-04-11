use crate::config::validate_collectors;
use crate::runtime::intern_collector;
use crate::BgpStreamElem;
use bgpkit_broker::BrokerError;
use bgpkit_parser::models::{Asn, Community, ElemType, MetaCommunity, NetworkPrefix, Origin};
use bgpkit_parser::BgpElem;
use bgpkit_parser::Filter;
// use bgpkit_parser::parse_ris_live_message;
use bgpkit_parser::rislive::error::ParserRisliveError;
use bgpkit_parser::rislive::messages::{
    RisLiveClientMessage, RisLiveMessage, RisMessageEnum, RisSubscribe,
};
use ipnet::IpNet;
use std::net::{Ipv4Addr, TcpStream};
use std::{thread, time};
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{connect, Message, WebSocket};

const RIS_LIVE_URL: &str = "ws://ris-live.ripe.net/v1/ws/?client=bgpflux";

// This macro is copied from bgpkit-parser
// Original source: https://github.com/bgpkit/bgpkit-parser/
// Copyright (c) 2021 Mingwei Zhang
// Licensed under the MIT License
// simple macro to make the code look a bit nicer
macro_rules! unwrap_or_return {
    ( $e:expr, $msg_string:expr ) => {
        match $e {
            Ok(x) => x,
            Err(_) => return Err(ParserRisliveError::IncorrectJson($msg_string)),
        }
    };
}

// This function is copied from bgpkit-parser
// Original source: https://github.com/bgpkit/bgpkit-parser/
// Copyright (c) 2021 Mingwei Zhang
// Licensed under the MIT License
/// parse prefix string into IpNet
fn parse_prefix(prefix_str: &str) -> Result<IpNet, ParserRisliveError> {
    let p = match prefix_str.parse::<IpNet>() {
        Ok(net) => net,
        Err(_) => {
            if prefix_str == "eor" {
                return Err(ParserRisliveError::ElemEndOfRibPrefix);
            }
            return Err(ParserRisliveError::ElemIncorrectPrefix(
                prefix_str.to_string(),
            ));
        }
    };
    Ok(p)
}

// This function is adapted from bgpkit-parser
// Original source: https://github.com/bgpkit/bgpkit-parser/
// Copyright (c) 2021 Mingwei Zhang
// Licensed under the MIT License
/// This function parses one message and returns a result of a vector of [BgpElem]s or an error
pub fn parse_ris_live_message(msg_str: &str) -> Result<Vec<BgpStreamElem>, ParserRisliveError> {
    let msg_string = msg_str.to_string();

    // parse RIS Live message to internal struct using serde.
    let msg: RisLiveMessage = match serde_json::from_str(msg_str) {
        Ok(m) => m,
        Err(_e) => return Err(ParserRisliveError::IncorrectJson(msg_string)),
    };

    match msg {
        RisLiveMessage::RisMessage(ris_msg) => {
            // we currently only handles the `ris_message` data type. other
            // types provides meta information, but reveals no BGP elements, and
            // thus for now will be ignored.

            if ris_msg.msg.is_none() {
                return Ok(vec![]);
            }

            let collector = intern_collector(ris_msg.host.split('.').take(1).collect());

            match ris_msg.msg.unwrap() {
                RisMessageEnum::UPDATE {
                    path,
                    community,
                    origin,
                    med,
                    aggregator,
                    announcements,
                    withdrawals,
                } => {
                    // Pre-allocate capacity based on announcements + withdrawals
                    let announce_count: usize = announcements
                        .as_ref()
                        .map(|a| a.iter().map(|ann| ann.prefixes.len()).sum())
                        .unwrap_or(0);
                    let withdraw_count: usize = withdrawals.as_ref().map(|w| w.len()).unwrap_or(0);
                    let mut elems: Vec<BgpStreamElem> =
                        Vec::with_capacity(announce_count + withdraw_count);

                    // parse community
                    let communities = community.map(|values| {
                        values
                            .into_iter()
                            .map(|(asn, data)| {
                                MetaCommunity::Plain(Community::Custom(Asn::new_32bit(asn), data))
                            })
                            .collect()
                    });

                    // parse origin
                    let bgp_origin = match origin {
                        None => None,
                        Some(o) => Some(match o.as_str() {
                            "igp" | "IGP" => Origin::IGP,
                            "egp" | "EGP" => Origin::EGP,
                            "incomplete" | "INCOMPLETE" => Origin::INCOMPLETE,
                            other => {
                                return Err(ParserRisliveError::ElemUnknownOriginType(
                                    other.to_string(),
                                ));
                            }
                        }),
                    };

                    // parse aggregator
                    let bgp_aggregator = match aggregator {
                        None => (None, None),
                        Some(aggr_str) => {
                            let (asn_str, ip_str) = match aggr_str.split_once(':') {
                                None => {
                                    return Err(ParserRisliveError::ElemIncorrectAggregator(
                                        aggr_str,
                                    ))
                                }
                                Some(v) => v,
                            };

                            let asn = unwrap_or_return!(asn_str.parse::<Asn>(), msg_string);
                            let ip = unwrap_or_return!(ip_str.parse::<Ipv4Addr>(), msg_string);
                            (Some(asn), Some(ip))
                        }
                    };

                    // parser announcements
                    if let Some(announcements) = announcements {
                        for announcement in announcements {
                            for prefix in &announcement.prefixes {
                                let p = parse_prefix(prefix.as_str())?;
                                let elem = BgpElem {
                                    timestamp: ris_msg.timestamp,
                                    elem_type: ElemType::ANNOUNCE,
                                    peer_ip: ris_msg.peer,
                                    peer_asn: ris_msg.peer_asn,
                                    peer_bgp_id: None,
                                    prefix: NetworkPrefix {
                                        prefix: p,
                                        path_id: None,
                                    },
                                    next_hop: Some(announcement.next_hop),
                                    as_path: path.clone(),
                                    origin_asns: None,
                                    origin: bgp_origin,
                                    local_pref: None,
                                    med,
                                    communities: communities.clone(),
                                    atomic: false,
                                    aggr_asn: bgp_aggregator.0,
                                    aggr_ip: bgp_aggregator.1,
                                    only_to_customer: None,
                                    unknown: None,
                                    deprecated: None,
                                };
                                elems.push(BgpStreamElem {
                                    collector_id: collector, //TODO replace with static_collector in lib!
                                    elem_type: elem.elem_type.into(),
                                    elem,
                                });
                            }
                        }
                    }

                    if let Some(withdrawals) = withdrawals {
                        for prefix in withdrawals {
                            // create new elems for withdrawals and push to elems
                            let p = parse_prefix(prefix.as_str())?;
                            let elem = BgpElem {
                                timestamp: ris_msg.timestamp,
                                elem_type: ElemType::WITHDRAW,
                                peer_ip: ris_msg.peer,
                                peer_asn: ris_msg.peer_asn,
                                peer_bgp_id: None,
                                prefix: NetworkPrefix {
                                    prefix: p,
                                    path_id: None,
                                },
                                next_hop: None,
                                as_path: None,
                                origin_asns: None,
                                origin: None,
                                local_pref: None,
                                med: None,
                                communities: None,
                                atomic: false,
                                aggr_asn: None,
                                aggr_ip: None,
                                only_to_customer: None,
                                unknown: None,
                                deprecated: None,
                            };
                            elems.push(BgpStreamElem {
                                collector_id: collector, //TODO replace with static_collector in lib!
                                elem_type: elem.elem_type.into(),
                                elem,
                            });
                        }
                    }

                    Ok(elems)
                }
                _ => Ok(vec![]),
            }
        }
        _ => Ok(vec![]),
    }
}

struct RisStream {
    // Configured socket
    socket: WebSocket<MaybeTlsStream<TcpStream>>,
    // This represents the "yield from" state:
    buffer: std::vec::IntoIter<BgpStreamElem>,
}

impl RisStream {
    fn new(socket: WebSocket<MaybeTlsStream<TcpStream>>) -> Self {
        let buffer = Vec::new().into_iter();
        Self { socket, buffer }
    }
}

impl Iterator for RisStream {
    type Item = BgpStreamElem;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // 1. Try to get an item from our "yield from" buffer
            if let Some(elem) = self.buffer.next() {
                return Some(elem);
            }

            // 2. If buffer is empty, get new data from socket
            let msg = self.socket.read().ok()?.to_string();

            // 3. Parse and refill the buffer (the "yield from" source)
            if let Ok(elems) = parse_ris_live_message(&msg) {
                self.buffer = elems.into_iter();
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct LiveConfig {
    /// Collector IDs to include in the stream
    pub collectors: Vec<String>,
    pub filters: Option<Vec<Filter>>,
}

impl LiveConfig {
    pub fn new(collectors: &[&str]) -> Result<Self, BrokerError> {
        let collectors = validate_collectors(collectors)?;

        Ok(Self {
            collectors,
            filters: None,
        })
    }
    /// Sets the filters for the BGP stream.
    pub fn with_filters(mut self, filters: Vec<Filter>) -> Self {
        self.filters = Some(filters);
        self
    }

    /// Add a single filter to the existing list
    pub fn add_filter(mut self, filter: Filter) -> Self {
        let mut filters = self.filters.unwrap_or_default();
        filters.push(filter);
        self.filters = Some(filters);
        self
    }
}

pub struct LiveBgpStream {
    pub config: LiveConfig,
}

impl LiveBgpStream {
    pub fn new(config: LiveConfig) -> Self {
        Self { config }
    }

    pub fn build(self) -> impl Iterator<Item = BgpStreamElem> {
        // connect to RIPE RIS Live websocket server
        let (mut socket, _response) =
            connect(RIS_LIVE_URL).expect("Can't connect to RIS Live websocket server");

        for collector in self.config.collectors {
            let msg = RisSubscribe::new().host(&collector);
            socket
                .send(Message::Text(msg.to_json_string().into()))
                .unwrap();
            thread::sleep(time::Duration::from_millis(50));
        }

        RisStream::new(socket)
    }
}
// impl
