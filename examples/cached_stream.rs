/// Stream BGP updates with local file caching.
///
/// On the first run, archive files are downloaded and saved to the cache directory.
/// Subsequent runs with the same time range skip the download entirely.
///
/// Run with:
/// ```sh
/// cargo run --example cached_stream
/// ```
use bgpflux::{BgpStream, BgpStreamConfig, DataType};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = BgpStreamConfig::new(
        "2025-01-15T12:00:00Z",
        "2025-01-15T13:00:00Z",
        &["route-views.wide", "rrc04"],
        DataType::Update,
    )?;

    let stream = BgpStream::new(config)
        .cache_dir("./bgp_cache")
        .build();

    let mut count = 0;
    for elem in stream {
        count += 1;
        if count <= 5 {
            println!("{}", elem);
        }
    }
    println!("Total elements: {}", count);

    Ok(())
}
