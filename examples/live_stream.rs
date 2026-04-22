/// Real-time BGP streaming with a jitter buffer.
///
/// Connects to RIS Live (WebSocket) and RouteViews Live (Kafka) simultaneously.
/// The jitter buffer reorders elements by timestamp with a 15-second window.
///
/// Run with:
/// ```sh
/// cargo run --example live_stream
/// ```
use bgpflux::{JitterBufferExt, LiveBgpStream, LiveConfig};
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = LiveConfig::new(&["rrc00", "route-views2"])?;

    let stream = LiveBgpStream::new(config)
        .build()
        .jitter_buffer(Duration::from_secs(15));

    println!("Streaming live BGP data (Ctrl+C to stop)...\n");

    for elem in stream {
        println!("{}", elem);
    }

    Ok(())
}
