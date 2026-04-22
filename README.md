# bgpflux

[![Docs](https://docs.rs/bgpflux/badge.svg)](https://docs.rs/bgpflux)
[![Crates.io](https://img.shields.io/crates/v/bgpflux.svg)](https://crates.io/crates/bgpflux)
[![CI](https://github.com/JustinLoye/bgpflux/actions/workflows/rust.yml/badge.svg)](https://github.com/JustinLoye/bgpflux/actions/workflows/rust.yml)
[![License](https://img.shields.io/github/license/JustinLoye/bgpflux.svg)](https://github.com/JustinLoye/bgpflux/blob/main/LICENSE)

A Rust library and CLI tool for historical BGP data analysis.  
Similar to [bgpreader](https://bgpstream.caida.org/docs/tools/bgpreader), it streams BGP elements in chronological order from:
- Multiple RIPE RIS and RouteViews route collectors
- Arbitrary time ranges
- Both RIBs and updates

## Features

- **Archive & Live streaming**: Query historical archives via [BGPKIT Broker](https://bgpkit.com/), or tap into real-time feeds from [RIS Live](https://ris-live.ripe.net/) (WebSocket) and [RouteViews Live](http://www.routeviews.org/) (Kafka)
- **Chronologically sorted output**: Elements from multiple collectors are merged in timestamp order via k-way merge
- **[High performance](performance.md)**: Throughput on par with bgpreader
- **Filtering**: Origin ASN, prefix, peer IP/ASN, AS path regex, community regex, IP version
- **Caching**: Optional local file caching to skip re-downloading archive data
- **Jitter buffer**: Reorder live stream elements into chronological order using a configurable delay window

## Installation

Add `bgpflux` to your `Cargo.toml` or install the CLI tool:

```bash
cargo install bgpflux
```

## Quick Start

### CLI — Archive Mode

Stream BGP updates from two collectors over a one-hour window:

```bash
bgpflux \
  --start "2025-01-15T12:00:00Z" \
  --end "2025-01-15T13:00:00Z" \
  -c route-views.wide,rrc04 \
  -t update
```

Stream RIB dumps:

```bash
bgpflux \
  --start "2025-01-15T12:00:00Z" \
  --end "2025-01-15T13:00:00Z" \
  -c route-views.wide,rrc04 \
  -t rib
```

Stream both RIBs and updates together:

```bash
bgpflux \
  --start "2025-01-15T12:00:00Z" \
  --end "2025-01-15T13:00:00Z" \
  -c route-views.wide,rrc04 \
  -t rib,update
```

Enable local caching to avoid re-downloading on repeated runs:

```bash
bgpflux \
  --start "2025-01-15T12:00:00Z" \
  --end "2025-01-15T13:00:00Z" \
  -c route-views.wide,rrc04 \
  -t update \
  --cache-dir ./bgp_cache
```

### CLI — Live Mode

Stream real-time BGP updates from RIS and RouteViews collectors:

```bash
bgpflux --live -c rrc00,route-views2
```

Use a jitter buffer (in seconds) to reorder elements by timestamp:

```bash
bgpflux --live -c rrc00,route-views2 --delay 15
```

### CLI — Filtering

Filter by origin ASN:

```bash
bgpflux --start "2025-01-15T12:00:00Z" --end "2025-01-15T13:00:00Z" \
  -c rrc00 -t update -o 13335
```

Filter by prefix:

```bash
bgpflux --start "2025-01-15T12:00:00Z" --end "2025-01-15T13:00:00Z" \
  -c rrc00 -t update -p 1.1.1.0/24
```

Filter by AS path regex:

```bash
bgpflux --start "2025-01-15T12:00:00Z" --end "2025-01-15T13:00:00Z" \
  -c rrc00 -t update -a "13335$"
```

IPv6 only:

```bash
bgpflux --start "2025-01-15T12:00:00Z" --end "2025-01-15T13:00:00Z" \
  -c rrc00 -t update -6
```

Generic filter expressions:

```bash
bgpflux --start "2025-01-15T12:00:00Z" --end "2025-01-15T13:00:00Z" \
  -c rrc00 -t update \
  -f "origin_asn!=13335" -f "peer_asn=15169"
```

### As a Library

```rust
use bgpflux::{BgpStream, BgpStreamConfig, DataType};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = BgpStreamConfig::new(
        "2025-01-15T12:00:00Z",
        "2025-01-15T13:00:00Z",
        &["route-views.wide", "rrc04"],
        DataType::Update,
    )?;

    let stream = BgpStream::new(config).build();

    for elem in stream {
        println!("{}", elem);
    }

    Ok(())
}
```

See the [`examples/`](examples/) directory for more:

| Example | Description |
|---------|-------------|
| [`basic_stream`](examples/basic_stream.rs) | Stream updates from multiple collectors |
| [`cached_stream`](examples/cached_stream.rs) | Use local caching to speed up repeated queries |
| [`filtered_stream`](examples/filtered_stream.rs) | Apply filters (origin ASN, prefix, AS path) |
| [`rib_snapshot`](examples/rib_snapshot.rs) | Collect unique prefixes from a RIB dump |
| [`live_stream`](examples/live_stream.rs) | Real-time BGP streaming with a jitter buffer |
| [`elem_fields`](examples/elem_fields.rs) | Access individual fields of BGP elements |

## Output Format

bgpflux outputs pipe-delimited records, one per line:

```
Type|Timestamp|PeerIP|PeerASN|Prefix|AsPath|Origin|NextHop|LocalPref|MED|Communities|Atomic|AggrASN|AggrIP|Collector
```

Where `Type` is `A` (announce), `W` (withdraw), or `R` (RIB entry). Example:

```
A|1736942400.469656|80.249.211.96|50629|193.186.6.0/24|50629 6939 47541|IGP|80.249.211.96|0|0|50629:500 50629:2000 50629:6939|false|||rrc04
W|1736942400.469656|80.249.211.96|50629|2a0e:b107:1a0::/44||||||||||rrc04
R|1736942400|80.249.211.96|50629|193.186.6.0/24|50629 6939 47541|IGP|80.249.211.96|0|0||false|||rrc04
```

## Core Components

### `BgpStream`

Main interface for streaming historical BGP data. Queries [BGPKIT Broker](https://bgpkit.com/) to discover archive files, downloads and parses them, and merges elements from all collectors in timestamp order.

```rust
use bgpflux::{BgpStream, BgpStreamConfig, DataType};

let config = BgpStreamConfig::new(
    "2025-01-15T12:00:00Z",
    "2025-01-15T13:00:00Z",
    &["route-views.wide"],
    DataType::Update,
).unwrap();

let stream = BgpStream::new(config)
    .cache_dir("./bgp_cache")  // optional
    .build();
```

### `BgpStreamConfig`

Configuration builder for archive streams. Supports multiple timestamp formats:

- RFC 3339: `"2025-01-15T12:00:00Z"`, `"2025-01-15T12:00:00+02:00"`
- ISO 8601 without timezone (assumed UTC): `"2025-01-15T12:00:00"`
- Date only (midnight UTC): `"2025-01-15"`, `"2025/01/15"`, `"20250115"`
- Unix timestamp: `"1736942400"`

### `BgpStreamElem`

A single BGP element. Wraps [`bgpkit_parser::BgpElem`](https://docs.rs/bgpkit-parser/latest/bgpkit_parser/models/struct.BgpElem.html) and adds a `collector_id` and `elem_type`. Implements `Deref<Target = BgpElem>`, so all `BgpElem` fields (timestamp, prefix, as_path, peer_ip, etc.) are directly accessible.

```rust
for elem in stream {
    println!(
        "{} {} via AS{} from {}",
        elem.elem_type, elem.prefix, elem.peer_asn, elem.collector_id
    );
}
```

### `LiveBgpStream`

Real-time streaming from RIS Live (WebSocket) and RouteViews Live (Kafka). Automatically routes collectors to the right backend based on their name (`rrc*` → RIS, `route-views*` → RouteViews).

```rust
use bgpflux::{LiveBgpStream, LiveConfig, JitterBufferExt};
use std::time::Duration;

let config = LiveConfig::new(&["rrc00", "route-views2"]).unwrap();
let stream = LiveBgpStream::new(config)
    .build()
    .jitter_buffer(Duration::from_secs(15));

for elem in stream {
    println!("{}", elem);
}
```

### `JitterBufferExt`

Extension trait on any `Iterator<Item = BgpStreamElem>`. Wraps the iterator in a binary-heap-based reordering buffer that holds elements for a configurable delay window before emitting them in timestamp order. Useful for live streams where elements from different collectors may arrive slightly out of order.

### Filtering

Filters are applied at the parser level using [bgpkit-parser filters](https://docs.rs/bgpkit-parser/latest/bgpkit_parser/struct.Filter.html). Both `BgpStreamConfig` and `LiveConfig` support `.with_filters()` and `.add_filter()`:

```rust
use bgpkit_parser::Filter;
use bgpflux::{BgpStreamConfig, DataType};

let config = BgpStreamConfig::new(
    "2025-01-15T12:00:00Z",
    "2025-01-15T13:00:00Z",
    &["rrc00"],
    DataType::Update,
)?
.add_filter(Filter::new("origin_asn", "13335")?)
.add_filter(Filter::new("ip_version", "ipv6")?);
```

Supported filter keys: `origin_asn`, `prefix`, `prefix_sub`, `prefix_super`, `peer_ip`, `peer_ips`, `peer_asn`, `as_path`, `community`, `ip_version`, `type`.

## Performance

bgpflux achieves throughput on par with [bgpreader](https://bgpstream.caida.org/docs/tools/bgpreader). See [performance.md](performance.md).

## Acknowledgments

This project uses code copied and adapted from:
- [bgpkit-broker](https://github.com/bgpkit/bgpkit-broker)
- [bgpkit-parser](https://github.com/bgpkit/bgpkit-parser)

## License

[MIT](LICENSE)
