# bgpflux

A Rust library and CLI tool for streaming ordered BGP elements from multiple route collectors with support for both RIB and update data types.

## Features

- **Streaming Architecture**: Efficiently process BGP data without loading everything into memory
- **Multi-Collector Support**: Aggregate BGP updates from multiple route collectors
- **Sorted Output**: All BGP elements are automatically merged in chronological order across collectors
- **Caching**: Optional local file caching to avoid re-downloading data
- **Flexible Data Types**: Support for both RIB dumps and Update messages
- **Customizable Filtering**: Filter by collectors, time ranges, and data types
- **High Performance**: Built with Rust for maximum speed and memory efficiency

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
bgpflux = "0.1"
```

Or install the CLI tool:

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
  --collectors route-views.wide,route-views.sydney \
  --data-type update
```

Stream RIB dumps

```bash
bgpflux \
  --start "2023-01-01T00:00:00Z" \
  --end "2023-01-01T01:00:00Z" \
  --collectors route-views.wide \
  --data-type rib
```

Stream both RIB and updates

```bash
bgpflux \
  --start "2023-01-01T00:00:00Z" \
  --end "2023-01-01T01:00:00Z" \
  --collectors route-views.wide \
  --data-type rib,update
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