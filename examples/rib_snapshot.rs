/// Collect unique prefixes from a RIB dump.
///
/// Demonstrates streaming RIB data and extracting prefix information.
///
/// Run with:
/// ```sh
/// cargo run --example rib_snapshot
/// ```
use bgpflux::{BgpStream, BgpStreamConfig, DataType};
use std::collections::HashSet;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = BgpStreamConfig::new(
        "2025-01-15T08:00:00Z",
        "2025-01-15T08:30:00Z",
        &["rrc04"],
        DataType::Rib,
    )?;

    let stream = BgpStream::new(config).build();

    let mut prefixes = HashSet::new();
    let mut total_entries = 0;

    for elem in stream {
        prefixes.insert(elem.prefix.to_string());
        total_entries += 1;
    }

    println!("RIB snapshot from rrc04:");
    println!("  Total RIB entries: {}", total_entries);
    println!("  Unique prefixes:   {}", prefixes.len());

    // Show a few sample prefixes
    println!("\nSample prefixes:");
    for prefix in prefixes.iter().take(10) {
        println!("  {}", prefix);
    }

    Ok(())
}
