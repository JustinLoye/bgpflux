use crate::elem::{BgpStreamElem, BgpStreamElemType};
use crate::utils::timestamp_from_project_url;
use bgpkit_parser::{BgpkitParser, ParserErrorWithBytes};
use itertools::Either;
use std::io::Read;

/// Processes a parser into a stream of BgpStreamElem.
pub(crate) fn process_parser_to_elems<R>(
    parser: BgpkitParser<R>,
    url: String,
    is_rib: bool,
    static_collector: &'static str,
    start: f64,
    end: f64,
) -> impl Iterator<Item = BgpStreamElem>
where
    R: Read + Send + 'static,
{
    // Initial filter applied to all parsers
    let base_iter = parser
        .into_iter()
        .take_while(move |elem| elem.timestamp <= end);

    // Branch on data type
    let mapped_iter = if is_rib {
        let rib_timestamp =
            timestamp_from_project_url(&url).expect("expected rib timestamp for rib file") as f64;

        // RIB specialized logic: replace timestamp (`change` to `dump`, like in bgpdump -t)
        Either::Left(base_iter.map(move |mut elem| {
            elem.timestamp = rib_timestamp;
            BgpStreamElem {
                collector_id: static_collector,
                elem_type: BgpStreamElemType::RIB,
                elem,
            }
        }))
    } else {
        // Updates specialized logic
        Either::Right(base_iter.map(move |elem| BgpStreamElem {
            collector_id: static_collector,
            elem_type: elem.elem_type.into(),
            elem,
        }))
    };

    // Final common filter (needs to be after to not be impacted by RIB timestamp logic)
    mapped_iter.skip_while(move |elem| elem.timestamp < start - 0.1)
}

pub(crate) fn init_parser_retry(
    url: &str,
    cache_dir: Option<&str>,
) -> Result<BgpkitParser<Box<dyn Read + Send>>, ParserErrorWithBytes>
where
{
    let mut attempts = 0;
    let max_attempts = 10;
    loop {
        let res = match cache_dir {
            Some(path) => BgpkitParser::new_cached(url, path),
            None => BgpkitParser::new(url),
        };
        match res {
            Ok(p) => return Ok(p),
            Err(e) if attempts < max_attempts - 1 => {
                attempts += 1;
                eprintln!("RETRYING {} (Attempt {}): {:?}", url, attempts, e);
                let delay_ms = 200u64
                    .saturating_mul(2u64.saturating_pow(attempts as u32))
                    .min(30_000);
                std::thread::sleep(std::time::Duration::from_millis(delay_ms));
            }
            Err(e) => return Err(e),
        }
    }
}
