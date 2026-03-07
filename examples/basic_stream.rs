/// Basic example of streaming BGP updates from multiple collectors.
///
/// Run with:
/// ```sh
/// cargo run --example basic_stream
/// ```
use bgpflux::{BgpStream, BgpStreamConfig, DataType};
use std::io::{self, BufWriter, Write};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure the stream: fetch BGP updates from two collectors
    // over a 1-hour period
    let config = BgpStreamConfig::new(
        "2010-09-01T00:00:00Z",
        "2010-09-01T01:00:00Z",
        vec!["route-views.wide", "route-views.sydney"],
        DataType::Update,
    )?;

    println!("Streaming BGP updates from {} collectors...", config.collectors.len());
    println!("Time range: {} to {}", config.ts_start, config.ts_end);
    println!();

    // Build the stream and process elements
    let stream = BgpStream::new(config).build();

    let stdout = io::stdout();
    let mut out = BufWriter::with_capacity(1 << 20, stdout.lock());

    let mut count = 0;
    for elem in stream {
        writeln!(out, "{}", elem)?;
        count += 1;

        // Print progress every 100,000 elements
        if count % 100_000 == 0 {
            eprintln!("Processed {} elements...", count);
        }
    }

    eprintln!("\nTotal elements processed: {}", count);
    Ok(())
}
