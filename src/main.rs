pub mod config;
pub mod elem;
pub mod utils;

use bgpflux::{BgpStream, BgpStreamConfig, DataType};
use clap::{Parser, ValueEnum};

#[derive(Parser, Debug)]
#[command(author, version, about = "A CLI to stream ordered BGP elements from multiple collectors", long_about = None)]
struct Args {
    /// Start timestamp (e.g., "2022-01-01T00:00:00Z" or Unix timestamp)
    #[arg(short, long)]
    start: String,

    /// End timestamp (e.g., "2022-01-01T01:00:00Z")
    #[arg(short, long)]
    end: String,

    /// Data type: 'update' or 'rib'
    #[arg(short = 't', long, value_enum, value_delimiter = ',')]
    data_type: Vec<DataTypeArg>,

    /// Collectors to filter by (can be used multiple times)
    #[arg(short, long, value_delimiter = ',')]
    collectors: Vec<String>,

    /// Custom broker URL
    #[arg(short, long, default_value = "https://api.bgpkit.com/v3/broker")]
    broker_url: String,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
enum DataTypeArg {
    #[value(alias = "updates")]
    Update,
    #[value(alias = "ribs")]
    Rib,
}

impl DataTypeArg {
    /// Collapses a collection of CLI args into a single library DataType
    fn to_data_type(args: &[DataTypeArg]) -> Result<DataType, &'static str> {
        let has_update = args.contains(&DataTypeArg::Update);
        let has_rib = args.contains(&DataTypeArg::Rib);

        match (has_update, has_rib) {
            (true, true) => Ok(DataType::Both),
            (true, false) => Ok(DataType::Update),
            (false, true) => Ok(DataType::Rib),
            _ => Err("Could not parse data type"),
        }
    }
}

use std::io::{self, BufWriter, Write};

fn main() {
    let args = Args::parse();

    let data_type = DataTypeArg::to_data_type(&args.data_type)
        .expect("Provide `update`, `rib` or both of them");

    let config = match BgpStreamConfig::new(args.start, args.end, args.collectors, data_type) {
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
