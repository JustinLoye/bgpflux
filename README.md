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

- **[High Performance](performance.md)**: Built with Rust for maximum speed
- **Robust Ecosystem**: Uses [BGPKIT](https://bgpkit.com/) parser and broker
- **Streaming**: Efficiently process BGP data on the fly without loading everything into memory
- **Caching**: Optional local file caching to avoid re-downloading archive data

## Installation

Add `bgpflux` to your `Cargo.toml` or install the CLI tool:

```bash
cargo install bgpflux
```

## Quick Start

### As a CLI Tool

Stream updates from specific collectors

```bash
bgpflux \
  --start "2010-09-01T00:00:00Z" \
  --end "2010-09-01T01:00:00Z" \
  --collectors route-views.wide,rrc04 \
  --data-type update
```

Stream RIB dumps

```bash
bgpflux \
  --start "2010-09-01T00:00:00Z" \
  --end "2010-09-01T01:00:00Z" \
  --collectors route-views.wide,rrc04 \
  --data-type rib
```

Stream both RIB and updates

```bash
bgpflux \
  --start "2010-09-01T00:00:00Z" \
  --end "2010-09-01T01:00:00Z" \
  --collectors route-views.wide,rrc04 \
  --data-type rib,update
```

Cache archive files

```bash
bgpflux \
  --start "2010-09-01T00:00:00Z" \
  --end "2010-09-01T01:00:00Z" \
  --collectors route-views.wide,rrc04 \
  --data-type update \
  --cache-dir cache
```

### As a Library

```rust
use bgpflux::{BgpStream, BgpStreamConfig, DataType};

fn main() {
    let config = BgpStreamConfig::new(
        "2010-09-01T00:00:00Z",
        "2010-09-01T01:00:00Z",
        vec!["route-views.wide", "route-views.sydney"],
        DataType::Update,
    ).unwrap();

    let stream = BgpStream::new(config).build();

    for elem in stream {
        println!("{}", elem);
    }
}
```

## Core Components

- **BgpStream**: Main streaming interface that aggregates data from multiple collectors
- **BgpStreamConfig**: Configuration for time ranges, collectors, and data types
- **BgpStreamElem**: Represents a single BGP element with metadata

## Acknowledgments

This project uses code adapted from:
- [bgpkit-broker](https://github.com/bgpkit/bgpkit-broker) for timestamp parsing
- [bgpkit-parser](https://github.com/bgpkit/bgpkit-parser) for BGP data parsing

## Roadmap

- [ ] live mode