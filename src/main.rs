pub mod config;
pub mod elem;
pub mod utils;

use bgpflux::{BgpStream, BgpStreamConfig, DataType};
use clap::{Parser, ValueEnum}; // Replace with your actual crate name

#[derive(Parser, Debug)]
#[command(author, version, about = "A CLI to stream BGP elements from collectors", long_about = None)]
struct Args {
    /// Start timestamp (e.g., "2022-01-01T00:00:00Z" or Unix timestamp)
    #[arg(short, long)]
    start: String,

    /// End timestamp (e.g., "2022-01-01T01:00:00Z")
    #[arg(short, long)]
    end: String,

    /// Data type: 'update' or 'rib'
    #[arg(short, long, value_enum, default_value_t = DataTypeArg::Update)]
    data_type: DataTypeArg,

    /// Collectors to filter by (can be used multiple times)
    #[arg(short, long, value_delimiter = ',')]
    collectors: Vec<String>,

    /// Custom broker URL
    #[arg(short, long, default_value = "https://api.bgpkit.com/v3/broker")]
    broker_url: String,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
enum DataTypeArg {
    Update,
    Rib,
}

impl From<DataTypeArg> for DataType {
    fn from(arg: DataTypeArg) -> Self {
        match arg {
            DataTypeArg::Update => DataType::Update,
            DataTypeArg::Rib => DataType::Rib,
        }
    }
}

use std::io::{self, BufWriter, Write};

fn main() {
    let args = Args::parse();

    let config =
        match BgpStreamConfig::new(args.start, args.end, args.collectors, args.data_type.into()) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Error configuring stream: {}", e);
                std::process::exit(1);
            }
        };

    let stream = BgpStream::new(config).broker_url(args.broker_url).build();

    let stdout = io::stdout();
    let mut out = BufWriter::with_capacity(1 << 20, stdout.lock());

    for elem in stream {
        writeln!(out, "{}", elem).unwrap();
    }
}
