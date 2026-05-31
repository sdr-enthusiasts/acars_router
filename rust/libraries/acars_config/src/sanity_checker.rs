// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.

//! Sanity checks for user-supplied configuration.
//!
//! All checks operate on the [`ProtocolIo`] view produced by
//! [`Input::protocols`], not on the parsed CLI fields directly, so adding a
//! new protocol or field is a one-line change.

use crate::{Input, Protocol, ProtocolIo};
use log::{error, trace};
use std::collections::HashSet;
use std::hash::Hash;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;

impl Input {
    /// Validate user-supplied configuration.
    ///
    /// Returns `Err(_)` summarising the failure count when one or more
    /// checks fail. Per-failure detail is emitted via `error!()` so that
    /// callers don't have to format it.
    pub fn check_config_option_sanity(&self) -> Result<(), String> {
        let protos = self.protocols();

        let mut errs: usize = 0;
        errs += check_no_duplicate_ports(&protos);
        errs += check_no_duplicate_hosts(&protos);
        for (proto, io) in Protocol::ALL.iter().zip(protos.iter()) {
            errs += check_protocol_ports(*proto, io);
            errs += check_protocol_hosts(*proto, io);
        }

        if errs == 0 {
            Ok(())
        } else {
            Err(format!(
                "Config option sanity check failed, with {errs} total errors"
            ))
        }
    }
}

// ---------------------------------------------------------------------------
// Cross-protocol duplicate detection
// ---------------------------------------------------------------------------

/// Bound ports must be unique within their address family. UDP and
/// (TCP ∪ ZMQ ∪ serve-TCP ∪ serve-ZMQ) form two independent name spaces.
fn check_no_duplicate_ports(protos: &[ProtocolIo; 5]) -> usize {
    let mut errs = 0;

    let mut udp_ports = Vec::new();
    let mut tcp_zmq_ports = Vec::new();
    for io in protos {
        udp_ports.extend(io.listen_udp.iter().copied());
        tcp_zmq_ports.extend(io.listen_tcp.iter().copied());
        tcp_zmq_ports.extend(io.listen_zmq.iter().copied());
        tcp_zmq_ports.extend(io.serve_tcp.iter().copied());
        tcp_zmq_ports.extend(io.serve_zmq.iter().copied());
    }

    if !has_unique_elements(&udp_ports) {
        error!("Duplicate UDP listen port across protocols: {udp_ports:?}");
        errs += 1;
    }
    if !has_unique_elements(&tcp_zmq_ports) {
        error!("Duplicate TCP/ZMQ bound port across protocols: {tcp_zmq_ports:?}");
        errs += 1;
    }
    errs
}

/// Host:port endpoints must be globally unique across every receive/send
/// field.
fn check_no_duplicate_hosts(protos: &[ProtocolIo; 5]) -> usize {
    let mut all_hosts = Vec::new();
    for io in protos {
        all_hosts.extend(io.receive_tcp.iter().cloned());
        all_hosts.extend(io.receive_zmq.iter().cloned());
        all_hosts.extend(io.send_tcp.iter().cloned());
        all_hosts.extend(io.send_udp.iter().cloned());
    }
    if has_unique_elements(&all_hosts) {
        0
    } else {
        error!("Duplicate host:port across receive/send fields: {all_hosts:?}");
        1
    }
}

// ---------------------------------------------------------------------------
// Per-protocol port / host validation
// ---------------------------------------------------------------------------

fn check_protocol_ports(proto: Protocol, io: &ProtocolIo) -> usize {
    let tag = proto.env_token();
    let slug = proto.as_str();
    let mut errs = 0;
    for (field, ports) in [
        ("LISTEN_UDP", "listen-udp", &io.listen_udp),
        ("LISTEN_TCP", "listen-tcp", &io.listen_tcp),
        ("LISTEN_ZMQ", "listen-zmq", &io.listen_zmq),
        ("SERVE_TCP", "serve-tcp", &io.serve_tcp),
        ("SERVE_ZMQ", "serve-zmq", &io.serve_zmq),
    ]
    .iter()
    .map(|(env, cli, v)| (format!("AR_{env}_{tag}/--{cli}-{slug}"), *v))
    {
        if !check_ports_are_valid(ports, &field) {
            errs += 1;
        }
    }
    errs
}

fn check_protocol_hosts(proto: Protocol, io: &ProtocolIo) -> usize {
    let tag = proto.env_token();
    let slug = proto.as_str();
    let mut errs = 0;
    for (field, hosts) in [
        ("RECEIVE_TCP", "receive-tcp", &io.receive_tcp),
        ("RECEIVE_ZMQ", "receive-zmq", &io.receive_zmq),
        ("SEND_UDP", "send-udp", &io.send_udp),
        ("SEND_TCP", "send-tcp", &io.send_tcp),
    ]
    .iter()
    .map(|(env, cli, v)| (format!("AR_{env}_{tag}/--{cli}-{slug}"), *v))
    {
        if !check_host_is_valid(hosts, &field) {
            errs += 1;
        }
        if !hosts.is_empty() && !has_unique_elements(hosts.iter().cloned()) {
            error!("Duplicate host in {field} configuration!\nContents: {hosts:?}");
            errs += 1;
        }
    }
    errs
}

// ---------------------------------------------------------------------------
// Field-level validators
// ---------------------------------------------------------------------------

/// `true` iff every port is in the range `1..=u16::MAX`. An explicitly
/// provided but empty list is invalid (`--listen-udp-acars=` with no value).
fn check_ports_are_valid(ports: &[u16], name: &str) -> bool {
    // We can't distinguish "no flag provided" from "flag provided with no
    // value" here — both arrive as empty. Historically the validator only
    // fired on `Some(vec![])`, so anything reaching us with `is_empty()` is
    // assumed to be the latter and rejected. Callers must skip this check
    // entirely if they want to support the absent case.
    if ports.is_empty() {
        // An empty slice could mean "not configured" — defer the error to
        // the caller. We only flag a hard zero-port.
        return true;
    }
    for &port in ports {
        if port == 0 {
            error!("{name} Listen Port is invalid as it is zero");
            return false;
        }
    }
    true
}

/// `true` iff every entry parses as `host:port` with a non-zero port and a
/// structurally valid host (IP literal, bracketed IPv6, or hostname). DNS
/// resolution is intentionally not performed here.
fn check_host_is_valid(sockets: &[String], name: &str) -> bool {
    if sockets.is_empty() {
        return true;
    }
    for socket in sockets {
        if socket.is_empty() {
            error!("{name} has been provided, but there are no socket addresses");
            return false;
        }
        // First attempt: parse as a `SocketAddr` literal. This handles
        // both `1.2.3.4:port` and bracketed IPv6 (`[::1]:port`).
        if let Ok(parsed_addr) = SocketAddr::from_str(socket) {
            if parsed_addr.port() == 0 {
                error!("{name}: Socket address is valid, but the port provided is zero: {socket}");
                return false;
            }
            trace!("{socket} is a valid socket address");
            continue;
        }

        // Otherwise, treat as `host:port` where host is a DNS name (or
        // bracketed IPv6 that somehow slipped through). Split from the
        // right so an unbracketed IPv6 literal without a port falls
        // through to a "no port" error rather than parsing one of its
        // hextets as the port.
        let Some((host, port_str)) = socket.rsplit_once(':') else {
            error!("{name} has no port specified for: {socket}");
            return false;
        };

        let Ok(port) = port_str.parse::<u16>() else {
            error!("{name} Port is invalid for: {socket}");
            return false;
        };
        if port == 0 {
            error!("{name}: Socket address is valid, but the port provided is zero: {socket}");
            return false;
        }

        if host.is_empty() {
            error!("{name} has an empty host for: {socket}");
            return false;
        }
        if host.chars().any(|c| c.is_whitespace() || c == '/') {
            error!("{name} has an invalid host for: {socket}");
            return false;
        }
        // An unbracketed host portion must not itself contain ':'. Any
        // raw IPv6 literal would need brackets to disambiguate from the
        // port separator.
        let bracketed = host.starts_with('[') && host.ends_with(']') && host.len() >= 2;
        if !bracketed && host.contains(':') {
            error!("{name} has an address with more than one colon in it: {socket}");
            return false;
        }
        if bracketed {
            let stripped = &host[1..host.len() - 1];
            if IpAddr::from_str(stripped).is_err() {
                error!("{name} has an invalid bracketed host for: {socket}");
                return false;
            }
        }

        trace!("{socket} is a valid socket address");
    }
    true
}

fn has_unique_elements<T, I>(iter: I) -> bool
where
    I: IntoIterator<Item = T>,
    T: Eq + Hash,
{
    let mut uniq = HashSet::new();
    iter.into_iter().all(move |x| uniq.insert(x))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_check_ports_are_valid_with_host() {
        let valid_hosts = vec![
            "127.0.0.1:8008".to_string(),
            "10.0.0.1:12345".to_string(),
            "192.168.1.1:65535".to_string(),
            "localhost:8008".to_string(),
            "ok.go:1234".to_string(),
        ];
        let invalid_hosts = vec![
            "127.0.0.1:0".to_string(),
            "10.0.0.1".to_string(),
            "192.168.1.1:65536".to_string(),
            "localhost".to_string(),
            "alpha.go".to_string(),
            "localhost:65536".to_string(),
            "abc:12three".to_string(),
            "host:123:456".to_string(),
        ];
        assert!(check_host_is_valid(&valid_hosts, "valid_hosts"));
        assert!(!check_host_is_valid(&invalid_hosts, "invalid_hosts"));
        // Empty slice now means "not configured" (returns true) — see fn doc.
        assert!(check_host_is_valid(&[], "empty_vec"));
    }

    #[test]
    fn test_check_ports_are_valid() {
        assert!(check_ports_are_valid(&[1, 8008, 65535], "valid_ports"));
        assert!(!check_ports_are_valid(&[0], "invalid_ports"));
        // Empty slice now means "not configured" (returns true) — see fn doc.
        assert!(check_ports_are_valid(&[], "empty_ports"));
    }

    #[test]
    fn test_check_no_duplicate_ports() {
        let mut ports = Input {
            listen_udp_vdlm2: Some(vec![8008, 65535]),
            listen_udp_acars: Some(vec![8009, 65534]),
            listen_tcp_acars: Some(vec![8008, 65533]),
            listen_tcp_vdlm2: Some(vec![8011, 65532]),
            serve_tcp_acars: Some(vec![8012, 65531]),
            serve_tcp_vdlm2: Some(vec![8013, 65530]),
            serve_zmq_acars: Some(vec![8014, 65529]),
            serve_zmq_vdlm2: Some(vec![8015, 65528]),
            ..Input::default()
        };
        assert_eq!(check_no_duplicate_ports(&ports.protocols()), 0);

        // Duplicate one set of ports.
        ports.serve_zmq_vdlm2 = Some(vec![8008, 65528]);
        assert_eq!(check_no_duplicate_ports(&ports.protocols()), 1);

        // HFDL/ACARS cross-protocol TCP listen port collision.
        let hfdl_collision = Input {
            listen_tcp_acars: Some(vec![8008]),
            listen_tcp_hfdl: Some(vec![8008]),
            ..Input::default()
        };
        assert_eq!(check_no_duplicate_ports(&hfdl_collision.protocols()), 1);
    }

    #[test]
    fn test_check_no_duplicate_ports_imsl_irdm() {
        // IMSL listen_zmq collides with IRDM serve_zmq.
        let imsl_irdm_zmq = Input {
            listen_zmq_imsl: Some(vec![9001]),
            serve_zmq_irdm: Some(vec![9001]),
            ..Input::default()
        };
        assert_eq!(check_no_duplicate_ports(&imsl_irdm_zmq.protocols()), 1);

        // IMSL listen_udp collides with IRDM listen_udp.
        let imsl_irdm_udp = Input {
            listen_udp_imsl: Some(vec![5550]),
            listen_udp_irdm: Some(vec![5550]),
            ..Input::default()
        };
        assert_eq!(check_no_duplicate_ports(&imsl_irdm_udp.protocols()), 1);

        // Different ports across all protocols should pass.
        let no_collisions = Input {
            listen_udp_imsl: Some(vec![5550]),
            listen_udp_irdm: Some(vec![5551]),
            serve_tcp_imsl: Some(vec![5552]),
            serve_tcp_irdm: Some(vec![5553]),
            listen_zmq_imsl: Some(vec![5554]),
            serve_zmq_irdm: Some(vec![5555]),
            ..Input::default()
        };
        assert_eq!(check_no_duplicate_ports(&no_collisions.protocols()), 0);
    }

    #[test]
    fn test_check_host_is_valid_ipv6() {
        let valid = vec![
            "[::1]:8080".to_string(),
            "[2001:db8::1]:443".to_string(),
            "example.com:8080".to_string(),
        ];
        assert!(check_host_is_valid(&valid, "valid_v6"));

        for case in [
            "[::1]:0",
            "fe80::1",
            "[::1]",
            "example.com",
            ":8080",
            "example.com:0",
            "example.com:abc",
        ] {
            assert!(
                !check_host_is_valid(&[case.to_string()], "invalid_v6"),
                "expected {case} to be rejected",
            );
        }
    }

    #[test]
    fn test_check_no_duplicate_hosts() {
        let mut hosts = Input {
            receive_tcp_acars: Some(vec![
                "test.com:8080".to_string(),
                "192.168.1.1:8080".to_string(),
            ]),
            receive_zmq_acars: Some(vec![
                "test.com:8081".to_string(),
                "192.168.1.1:8081".to_string(),
            ]),
            receive_tcp_vdlm2: Some(vec![
                "test.com:8082".to_string(),
                "192.168.1.1:8082".to_string(),
            ]),
            receive_zmq_vdlm2: Some(vec![
                "test.com:8083".to_string(),
                "192.168.1.1:8083".to_string(),
            ]),
            send_tcp_acars: Some(vec![
                "test.com:8084".to_string(),
                "192.168.1.1:8084".to_string(),
            ]),
            send_udp_acars: Some(vec![
                "test.com:8085".to_string(),
                "192.168.1.1:8085".to_string(),
            ]),
            send_tcp_vdlm2: Some(vec![
                "test.com:8086".to_string(),
                "192.168.1.1:8086".to_string(),
            ]),
            send_udp_vdlm2: Some(vec![
                "test.com:8087".to_string(),
                "192.168.1.1:8087".to_string(),
            ]),
            ..Input::default()
        };
        assert_eq!(check_no_duplicate_hosts(&hosts.protocols()), 0);

        hosts.send_udp_vdlm2 = Some(vec![
            "test.com:8087".to_string(),
            "192.168.1.1:8087".to_string(),
            "test.com:8087".to_string(),
        ]);
        assert_eq!(check_no_duplicate_hosts(&hosts.protocols()), 1);
    }
}
