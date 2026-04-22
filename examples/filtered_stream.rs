/// Apply filters to a BGP stream.
///
/// Demonstrates filtering by origin ASN, prefix, and AS path regex.
/// Filters are applied at the parser level for efficiency.
///
/// Run with:
/// ```sh
/// cargo run --example filtered_stream
/// ```
use bgpflux::{BgpStream, BgpStreamConfig, DataType};
use bgpkit_parser::Filter;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Filter for routes originated by AS13335 (Cloudflare)
    let config = BgpStreamConfig::new(
        "2025-01-15T12:00:00Z",
        "2025-01-15T13:00:00Z",
        &["rrc00"],
        DataType::Update,
    )?
    .add_filter(Filter::new("origin_asn", "13335")?);

    println!("=== Updates originated by AS13335 ===");
    let mut count = 0;
    for elem in BgpStream::new(config).build() {
        println!("{}", elem);
        count += 1;
    }
    println!("({} elements)\n", count);

    // Filter for a specific prefix with AS path regex
    let config = BgpStreamConfig::new(
        "2025-01-15T12:00:00Z",
        "2025-01-15T13:00:00Z",
        &["rrc00"],
        DataType::Update,
    )?
    .add_filter(Filter::new("prefix", "1.1.1.0/24")?)
    .add_filter(Filter::new("as_path", "13335$")?);

    println!("=== Updates for 1.1.1.0/24 with AS path ending in 13335 ===");
    for elem in BgpStream::new(config).build() {
        println!("{}", elem);
    }

    // Filter for IPv6 only
    let config = BgpStreamConfig::new(
        "2025-01-15T12:00:00Z",
        "2025-01-15T13:00:00Z",
        &["rrc00"],
        DataType::Update,
    )?
    .add_filter(Filter::new("ip_version", "ipv6")?);

    println!("\n=== IPv6 updates (first 10) ===");
    for elem in BgpStream::new(config).build().take(10) {
        println!("{}", elem);
    }

    Ok(())
}
