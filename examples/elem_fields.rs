/// Access individual fields of BGP elements.
///
/// BgpStreamElem implements Deref<Target = BgpElem>, so all underlying fields
/// (timestamp, prefix, as_path, peer_ip, peer_asn, origin, next_hop, etc.)
/// are directly accessible.
///
/// Run with:
/// ```sh
/// cargo run --example elem_fields
/// ```
use bgpflux::{BgpStream, BgpStreamConfig, BgpStreamElemType, DataType};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = BgpStreamConfig::new(
        "2025-01-15T12:00:00Z",
        "2025-01-15T13:00:00Z",
        &["rrc04"],
        DataType::Update,
    )?;

    let stream = BgpStream::new(config).build();

    for elem in stream.take(20) {
        // elem_type and collector_id are bgpflux-specific fields
        let direction = match elem.elem_type {
            BgpStreamElemType::ANNOUNCE => "ANNOUNCE",
            BgpStreamElemType::WITHDRAW => "WITHDRAW",
            BgpStreamElemType::RIB => "RIB",
        };

        // All other fields come from BgpElem via Deref
        println!(
            "[{}] {} from collector {}",
            direction, elem.prefix, elem.collector_id
        );
        println!("  Timestamp: {}", elem.timestamp);
        println!("  Peer:      {} (AS{})", elem.peer_ip, elem.peer_asn);

        if let Some(ref path) = elem.as_path {
            println!("  AS Path:   {}", path);
        }
        if let Some(ref next_hop) = elem.next_hop {
            println!("  Next Hop:  {}", next_hop);
        }
        if let Some(ref communities) = elem.communities {
            if !communities.is_empty() {
                println!("  Communities: {:?}", communities);
            }
        }
        println!();
    }

    Ok(())
}
