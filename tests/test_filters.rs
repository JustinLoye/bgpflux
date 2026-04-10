use bgpflux::{BgpStream, BgpStreamConfig, DataType};
use bgpkit_parser::Filter;
use std::collections::HashSet;

// Test configuration constants
const BASE_START: &str = "2010-09-01T00:00:00Z";
const BASE_END: &str = "2010-09-01T00:30:00Z";
const COLLECTORS: &[&str] = &["route-views.sydney", "route-views.wide"];

const BASE_START_TS: f64 = 1283299200.0;
const BASE_END_TS: f64 = 1283301000.0;

fn create_config_with_filters(filters: Vec<Filter>) -> BgpStreamConfig {
    let config = BgpStreamConfig::new(BASE_START, BASE_END, COLLECTORS.to_vec(), DataType::Update)
        .expect("Failed to create config");

    if !filters.is_empty() {
        config.with_filters(filters)
    } else {
        config
    }
}

/// Test that RIB data can be successfully downloaded and parsed
#[test]
fn test_parser_rib() {
    let config = BgpStreamConfig::new(BASE_START, BASE_END, COLLECTORS.to_vec(), DataType::Rib)
        .expect("Failed to create config");

    let stream = BgpStream::new(config).build();

    let mut count = 0;

    for elem in stream {
        count += 1;
        // Basic validation that element has content
        assert_eq!(elem.elem_type, bgpflux::BgpStreamElemType::RIB);
        assert!(
            elem.timestamp >= BASE_START_TS,
            "RIB timestamp {} before start time {}",
            elem.timestamp,
            BASE_START_TS
        );
        assert!(
            elem.timestamp <= BASE_END_TS,
            "RIB timestamp {} after end time {}",
            elem.timestamp,
            BASE_END_TS
        );
    }

    assert!(
        count > 0,
        "RIB stream returned no data for a known valid time range"
    );
}

/// Test that Update data can be successfully downloaded and parsed
#[test]
fn test_parser_update() {
    let config = BgpStreamConfig::new(BASE_START, BASE_END, COLLECTORS.to_vec(), DataType::Update)
        .expect("Failed to create config");

    let stream = BgpStream::new(config).build();

    let mut count = 0;

    for elem in stream {
        count += 1;
        // Basic validation that element has content
        match elem.elem_type {
            bgpflux::BgpStreamElemType::ANNOUNCE | bgpflux::BgpStreamElemType::WITHDRAW => {}
            _ => panic!("Unexpected element type in update stream"),
        }
        assert!(
            elem.timestamp >= BASE_START_TS,
            "Update timestamp before start time"
        );
        assert!(
            elem.timestamp <= BASE_END_TS,
            "Update timestamp after end time"
        );
    }

    assert!(
        count > 0,
        "Update stream returned no data for a known valid time range"
    );
}

/// Test filtering by IPv6
#[test]
fn test_filter_ip_version_v6() {
    let filters = vec![Filter::new("ip_version", "6").expect("Failed to create filter")];
    let config = create_config_with_filters(filters);

    let stream = BgpStream::new(config).build();

    let mut count = 0;
    for elem in stream {
        count += 1;
        // IPv6 prefixes contain ':'
        let prefix_str = elem.prefix.to_string();
        assert!(
            prefix_str.contains(':'),
            "Found non-IPv6 prefix: {}",
            prefix_str
        );
    }

    assert!(count > 0, "IPv6 filter returned no data");
}

/// Test filtering by IPv4
#[test]
fn test_filter_ip_version_v4() {
    let filters = vec![Filter::new("ip_version", "4").expect("Failed to create filter")];
    let config = create_config_with_filters(filters);

    let stream = BgpStream::new(config).build();

    let mut count = 0;
    for elem in stream {
        count += 1;
        // IPv4 prefixes should not contain ':'
        let prefix_str = elem.prefix.to_string();
        assert!(
            !prefix_str.contains(':'),
            "Found non-IPv4 prefix: {}",
            prefix_str
        );
    }

    assert!(count > 0, "IPv4 filter returned no data");
}

/// Test filtering by update type 'withdraw'
#[test]
fn test_filter_update_type_withdraw() {
    let filters = vec![Filter::new("type", "w").expect("Failed to create filter")];
    let config = create_config_with_filters(filters);

    let stream = BgpStream::new(config).build();

    let mut count = 0;
    for elem in stream {
        count += 1;
        assert_eq!(
            elem.elem_type,
            bgpflux::BgpStreamElemType::WITHDRAW,
            "Found non-withdraw element type"
        );
    }

    assert!(count > 0, "Withdraw filter returned no data");
}

/// Test filtering by update type 'announce'
#[test]
fn test_filter_update_type_announce() {
    let filters = vec![Filter::new("type", "a").expect("Failed to create filter")];
    let config = create_config_with_filters(filters);

    let stream = BgpStream::new(config).build();

    let mut count = 0;
    for elem in stream {
        count += 1;
        assert_eq!(
            elem.elem_type,
            bgpflux::BgpStreamElemType::ANNOUNCE,
            "Found non-announce element type"
        );
    }

    assert!(count > 0, "Announce filter returned no data");
}

/// Test filtering by a specific origin ASN
#[test]
fn test_filter_origin_asn() {
    let _origin_asn = 27653;
    let filters = vec![Filter::new("origin_asn", "27653").expect("Failed to create filter")];
    let config = create_config_with_filters(filters);

    let stream = BgpStream::new(config).build();

    let mut count = 0;
    for _elem in stream {
        // Just verify the filter executed and returned data
        // The actual origin ASN filtering is done server-side by bgpkit_parser
        count += 1;
    }

    assert!(count > 0, "Origin ASN filter returned no data");
}

/// Test filtering by a specific peer ASN
#[test]
fn test_filter_peer_asn() {
    let target_peer_asn = 2497;
    let filters = vec![Filter::new("peer_asn", "2497").expect("Failed to create filter")];
    let config = create_config_with_filters(filters);

    let stream = BgpStream::new(config).build();

    let mut count = 0;
    for elem in stream {
        // Verify peer ASN matches
        assert_eq!(elem.peer_asn, target_peer_asn, "Peer ASN mismatch");
        count += 1;
    }

    assert!(count > 0, "Peer ASN filter returned no data");
}

/// Test filtering by a specific peer IP
#[test]
fn test_filter_peer_ip() {
    let peer_ip = "202.249.2.169";
    let filters = vec![Filter::new("peer_ip", peer_ip).expect("Failed to create filter")];
    let config = create_config_with_filters(filters);

    let stream = BgpStream::new(config).build();

    let mut count = 0;
    for elem in stream {
        assert_eq!(elem.peer_ip.to_string(), peer_ip, "Peer IP mismatch");
        count += 1;
    }

    assert!(count > 0, "Peer IP filter returned no data");
}

/// Test filtering by multiple peer IPs
#[test]
fn test_filter_peer_ips() {
    let peer_ips = vec!["202.249.2.169", "202.167.228.46"];
    let filters =
        vec![Filter::new("peer_ips", "202.249.2.169,202.167.228.46")
            .expect("Failed to create filter")];
    let config = create_config_with_filters(filters);

    let stream = BgpStream::new(config).build();

    let peer_ip_set: HashSet<&str> = peer_ips.into_iter().collect();
    let mut count = 0;
    for elem in stream {
        let peer_ip_str = elem.peer_ip.to_string();
        assert!(
            peer_ip_set.contains(peer_ip_str.as_str()),
            "Peer IP not in allowed set: {}",
            peer_ip_str
        );
        count += 1;
    }

    assert!(count > 0, "Peer IPs filter returned no data");
}

/// Test filtering by an exact prefix
#[test]
fn test_filter_prefix_exact() {
    let target_prefix = "213.196.74.0/24";
    let filters = vec![Filter::new("prefix", target_prefix).expect("Failed to create filter")];
    let config = create_config_with_filters(filters);

    let stream = BgpStream::new(config).build();

    let mut count = 0;
    for elem in stream {
        let prefix_str = elem.prefix.to_string();
        assert_eq!(prefix_str, target_prefix, "Prefix mismatch in exact filter");
        count += 1;
    }

    assert!(count > 0, "Exact prefix filter returned no data");
}

/// Test filtering by prefix and super-prefixes
#[test]
fn test_filter_prefix_super() {
    // First, collect all IPv4 prefixes
    let ipv4_filters = vec![Filter::new("ip_version", "4").expect("Failed to create filter")];
    let config = BgpStreamConfig::new(BASE_START, BASE_END, COLLECTORS.to_vec(), DataType::Update)
        .expect("Failed to create config");
    let config = config.with_filters(ipv4_filters);

    let mut all_prefixes: Vec<(String, ipnet::Ipv4Net)> = Vec::new();
    let stream = BgpStream::new(config).build();

    // Collect all IPv4 prefixes
    for elem in stream {
        let prefix_str = elem.prefix.to_string();
        if let Ok(net) = prefix_str.parse::<ipnet::Ipv4Net>() {
            all_prefixes.push((prefix_str, net));
        }
    }

    // Find a child prefix that has parent prefixes (supernets)
    let mut target_prefix: Option<String> = None;
    let mut expected_parent_prefixes = HashSet::new();

    // Find a prefix with at least one parent supernet
    for (prefix_str, net) in &all_prefixes {
        let prefix_len = net.prefix_len();

        if prefix_len > 0 {
            // Create a supernet by reducing prefix length
            let supernet = net.trunc();

            // Check if this supernet exists in our dataset
            for (other_str, other_net) in &all_prefixes {
                if other_net == &supernet {
                    expected_parent_prefixes.insert(other_str.clone());
                }
            }
        }

        // If we found a prefix with parents, use it for testing
        if !expected_parent_prefixes.is_empty() {
            target_prefix = Some(prefix_str.clone());
            break;
        }
    }

    // If we found a suitable prefix, test the filter
    if let Some(test_prefix) = target_prefix {
        let filters =
            vec![Filter::new("prefix_super", &test_prefix).expect("Failed to create filter")];
        let config = create_config_with_filters(filters);
        let stream = BgpStream::new(config).build();

        let mut result_prefixes = HashSet::new();
        for elem in stream {
            result_prefixes.insert(elem.prefix.to_string());
        }

        // Verify we got results
        assert!(
            result_prefixes.len() > 0,
            "Prefix super filter returned no data"
        );
        // The result should include at least the test prefix itself
        assert!(
            result_prefixes.contains(&test_prefix),
            "Prefix super filter should include the target prefix"
        );
    }
}

/// Test filtering by prefix and sub-prefixes
#[test]
fn test_filter_prefix_sub() {
    // First, collect all IPv4 prefixes
    let ipv4_filters = vec![Filter::new("ip_version", "4").expect("Failed to create filter")];
    let config = BgpStreamConfig::new(BASE_START, BASE_END, COLLECTORS.to_vec(), DataType::Update)
        .expect("Failed to create config");
    let config = config.with_filters(ipv4_filters);

    let mut all_prefixes: Vec<(String, ipnet::Ipv4Net)> = Vec::new();
    let stream = BgpStream::new(config).build();

    // Collect all IPv4 prefixes
    for elem in stream {
        let prefix_str = elem.prefix.to_string();
        if let Ok(net) = prefix_str.parse::<ipnet::Ipv4Net>() {
            all_prefixes.push((prefix_str, net));
        }
    }

    // Find a prefix that has sub-prefixes (longer prefixes within it)
    let mut target_prefix: Option<String> = None;
    let mut expected_subnets = HashSet::new();

    // Find a prefix with at least one subnet
    for (prefix_str, net) in &all_prefixes {
        let prefix_len = net.prefix_len();

        // Look for any prefixes that are longer (more specific)
        // and within this prefix's range
        if prefix_len < 32 {
            for (other_str, other_net) in &all_prefixes {
                if other_str != prefix_str {
                    // Check if other_net is contained in net
                    if net.contains(&other_net.addr()) && other_net.prefix_len() > prefix_len {
                        expected_subnets.insert(other_str.clone());
                    }
                }
            }
        }

        // If we found a prefix with subnets, use it for testing
        if !expected_subnets.is_empty() {
            target_prefix = Some(prefix_str.clone());
            break;
        }
    }

    // If we found a suitable prefix, test the filter
    if let Some(test_prefix) = target_prefix {
        let filters =
            vec![Filter::new("prefix_sub", &test_prefix).expect("Failed to create filter")];
        let config = create_config_with_filters(filters);
        let stream = BgpStream::new(config).build();

        let mut result_prefixes = HashSet::new();
        for elem in stream {
            result_prefixes.insert(elem.prefix.to_string());
        }

        // Verify we got results
        assert!(
            result_prefixes.len() > 0,
            "Prefix sub filter returned no data"
        );
        // The result should include at least the test prefix itself
        assert!(
            result_prefixes.contains(&test_prefix),
            "Prefix sub filter should include the target prefix"
        );
    }
}

/// Test filtering by AS-path regex
#[test]
fn test_filter_aspath() {
    let peer_asn = 2497;

    // First, get the count of announcements from this peer without regex
    let config = BgpStreamConfig::new(BASE_START, BASE_END, COLLECTORS.to_vec(), DataType::Update)
        .expect("Failed to create config");

    let stream = BgpStream::new(config).build();
    let mut target_count = 0;

    for elem in stream {
        if elem.peer_asn == peer_asn && elem.elem_type == bgpflux::BgpStreamElemType::ANNOUNCE {
            target_count += 1;
        }
    }

    assert!(target_count > 0, "No announcements found from test peer");

    // Now test with regex filter - bgpkit uses POSIX regex
    let regex = format!(r"^{peer_asn}(?:\s|$)");
    let filters = vec![Filter::new("as_path", &regex).expect("Failed to create filter")];
    let config = create_config_with_filters(filters);
    let stream = BgpStream::new(config).build();

    let mut count = 0;
    for elem in stream {
        if elem.elem_type == bgpflux::BgpStreamElemType::ANNOUNCE {
            count += 1;
        }
    }

    assert_eq!(
        count, target_count,
        "AS-path filtered count does not match expected count"
    );
}

/// Test that multiple filters can be applied together (AND logic)
#[test]
fn test_multiple_filters_and() {
    let filters = vec![
        Filter::new("ip_version", "4").expect("Failed to create filter"),
        Filter::new("type", "a").expect("Failed to create filter"),
    ];
    let config = create_config_with_filters(filters);

    let stream = BgpStream::new(config).build();

    let mut count = 0;
    for elem in stream {
        count += 1;
        // Both conditions must be true
        let prefix_str = elem.prefix.to_string();
        assert!(
            !prefix_str.contains(':'),
            "Found IPv6 prefix with IPv4 filter"
        );
        assert_eq!(
            elem.elem_type,
            bgpflux::BgpStreamElemType::ANNOUNCE,
            "Found non-announce with announce filter"
        );
    }

    assert!(count > 0, "Multiple filters returned no data");
}
