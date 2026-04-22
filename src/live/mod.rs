use crate::config::validate_collectors;
use crate::elem::BgpStreamElem;
use bgpkit_broker::BrokerError;
use bgpkit_parser::Filter;
#[cfg(all(feature = "live-ris", feature = "live-routeviews"))]
use std::{sync::mpsc, thread};

mod jitter_buffer;
#[cfg(feature = "live-ris")]
mod ris;
#[cfg(feature = "live-routeviews")]
mod routeviews;

#[cfg(feature = "live-ris")]
use ris::RisStream;
#[cfg(feature = "live-routeviews")]
use routeviews::RouteviewsStream;

pub use jitter_buffer::JitterBufferExt;

#[derive(Debug, Clone)]
pub struct LiveConfig {
    /// Collector IDs to include in the stream
    pub collectors: Vec<String>,
    /// Filter options (same as bgpkit-parser)
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
    ris_collectors: Vec<String>,
    rv_collectors: Vec<String>,
}

impl LiveBgpStream {
    pub fn new(config: LiveConfig) -> Self {
        let (ris_collectors, rv_collectors): (Vec<String>, Vec<String>) = config
            .collectors
            .clone()
            .into_iter()
            .partition(|c| c.contains("rrc"));
        Self {
            config,
            ris_collectors,
            rv_collectors,
        }
    }

    pub fn build(self) -> Box<dyn Iterator<Item = BgpStreamElem>> {
        let ris_collectors = self.ris_collectors;
        let rv_collectors = self.rv_collectors;

        #[cfg(not(feature = "live-ris"))]
        if !ris_collectors.is_empty() {
            eprintln!(
                "Warning: RIS Live collectors {:?} require the 'live-ris' feature (ignored)",
                ris_collectors
            );
        }
        #[cfg(not(feature = "live-routeviews"))]
        if !rv_collectors.is_empty() {
            eprintln!(
                "Warning: RouteViews Live collectors {:?} require the 'live-routeviews' feature (ignored)",
                rv_collectors
            );
        }

        #[cfg(all(feature = "live-ris", feature = "live-routeviews"))]
        if !ris_collectors.is_empty() && !rv_collectors.is_empty() {
            let (tx, rx) = mpsc::channel();

            let tx_ris = tx.clone();
            thread::spawn(move || {
                for elem in RisStream::new(&ris_collectors) {
                    if tx_ris.send(elem).is_err() {
                        break;
                    }
                }
            });

            let tx_rv = tx.clone();
            thread::spawn(move || {
                for elem in RouteviewsStream::new(&rv_collectors) {
                    if tx_rv.send(elem).is_err() {
                        break;
                    }
                }
            });

            drop(tx);
            return Box::new(rx.into_iter());
        }

        #[cfg(feature = "live-ris")]
        if !ris_collectors.is_empty() {
            return Box::new(RisStream::new(&ris_collectors));
        }

        #[cfg(feature = "live-routeviews")]
        if !rv_collectors.is_empty() {
            return Box::new(RouteviewsStream::new(&rv_collectors));
        }

        Box::new(std::iter::empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use std::{collections::HashSet, time::Duration};

    /// Consume the live stream to check if the delay is small and if the requested collectors are present
    fn check_livestream(
        stream: impl Iterator<Item = BgpStreamElem>,
        config: LiveConfig,
    ) -> Vec<BgpStreamElem> {
        let test_start = Utc::now().timestamp_micros() as f64 / 1e6;
        let test_time = 30.0;
        let max_delay = 45.0;

        let mut seen_collectors = HashSet::new();
        let mut elems = Vec::new();
        for elem in stream {
            let now = Utc::now().timestamp_micros() as f64 / 1e6;
            if now > test_start + test_time {
                break;
            }
            seen_collectors.insert(elem.collector_id.to_string());
            let delay = now - elem.timestamp;
            assert!(
                delay < max_delay,
                "Live stream as more than {} delay (s) (observed {} s)",
                max_delay as i64,
                delay as i64
            );
            elems.push(elem);
        }

        // Check if config and stream match
        assert_eq!(seen_collectors, HashSet::from_iter(config.collectors));
        elems
    }

    #[test]
    #[cfg(feature = "live-ris")]
    fn test_ris() {
        let config = LiveConfig::new(&["rrc00", "rrc21"]).unwrap();
        let stream = LiveBgpStream::new(config.clone()).build();
        check_livestream(stream, config);
    }

    #[test]
    #[cfg(feature = "live-routeviews")]
    fn test_rv() {
        let config = LiveConfig::new(&["route-views2", "amsix.ams"]).unwrap();
        let stream = LiveBgpStream::new(config.clone()).build();
        check_livestream(stream, config);
    }

    #[test]
    #[cfg(all(feature = "live-ris", feature = "live-routeviews"))]
    fn test_both() {
        let config = LiveConfig::new(&["route-views2", "rrc00"]).unwrap();
        let stream = LiveBgpStream::new(config.clone()).build();
        check_livestream(stream, config);
    }

    #[test]
    #[cfg(all(feature = "live-ris", feature = "live-routeviews"))]
    fn test_jitter_buffer() {
        let config = LiveConfig::new(&["route-views2", "rrc00"]).unwrap();
        let stream = LiveBgpStream::new(config.clone())
            .build()
            .jitter_buffer(Duration::from_secs(20));

        let elems = check_livestream(stream, config);
        assert!(elems.is_sorted());
    }
}
