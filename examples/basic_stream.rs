/// Basic example of streaming BGP updates from multiple collectors.
///
/// Run with:
/// ```sh
/// cargo run --example basic_stream
/// ```
use bgpflux::{BgpStream, BgpStreamConfig, DataType};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = BgpStreamConfig::new(
        "2010-09-01T00:00:00Z",
        "2010-09-01T01:00:00Z",
        vec!["route-views.wide", "rrc04"],
        DataType::Update,
    )?;

    let stream = BgpStream::new(config).build();

    for elem in stream {
        println!("{}", elem);
    }

    Ok(())
}
