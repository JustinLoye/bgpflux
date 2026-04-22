/// Stream BGP updates from multiple collectors.
///
/// This is the simplest way to use bgpflux: configure a time range, pick
/// some collectors, and iterate over the chronologically sorted elements.
///
/// Run with:
/// ```sh
/// cargo run --example basic_stream
/// ```
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
