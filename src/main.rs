pub mod cli;
pub mod config;
pub mod elem;
pub mod utils;

use bgpflux::{
    live::{JitterBufferExt, LiveBgpStream, LiveConfig},
    BgpStream, BgpStreamConfig,
};
use clap::Parser;
use cli::Args;
use std::{
    io::{self, BufWriter, Write},
    time::Duration,
};

fn main() {
    let args = Args::parse();

    let filters = args.filters.parse().unwrap_or_else(|e| {
        eprintln!("Error parsing filters: {}", e);
        std::process::exit(1);
    });

    let collectors: Vec<&str> = args.collector.iter().map(String::as_str).collect();

    if args.live {
        let mut config = LiveConfig::new(&collectors).expect("Invalid collectors");

        if !filters.is_empty() {
            config = config.with_filters(filters);
        }
        let stream = LiveBgpStream::new(config);

        if let Some(d) = args.delay {
            for elem in stream.build().jitter_buffer(Duration::from_secs_f64(d)) {
                println!("{elem}");
            }
        } else {
            for elem in stream.build() {
                println!("{elem}");
            }
        }
    } else {
        let data_type = cli::DataTypeArg::to_data_type(
            &args
                .data_type
                .expect("Data type should be provided for archive mode"),
        )
        .unwrap_or_else(|e| {
            eprintln!("Error configuring stream: {}", e);
            std::process::exit(1);
        });

        let mut config = BgpStreamConfig::new(
            args.start.unwrap(),
            args.end.unwrap(),
            &collectors,
            data_type,
        )
        .unwrap_or_else(|e| {
            eprintln!("Error configuring stream: {}", e);
            std::process::exit(1);
        });

        if !filters.is_empty() {
            config = config.with_filters(filters);
        }

        let mut stream = BgpStream::new(config);

        if let Some(cache_dir) = args.cache_dir {
            stream = stream.cache_dir(cache_dir);
        }
        if let Some(broker_url) = args.broker_url {
            stream = stream.broker_url(broker_url);
        }

        let stdout = io::stdout();
        let mut out = BufWriter::with_capacity(1 << 20, stdout.lock());

        for elem in stream.build() {
            writeln!(out, "{}", elem).unwrap();
        }
    }
}
