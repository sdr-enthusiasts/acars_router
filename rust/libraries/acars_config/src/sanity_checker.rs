// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

// File to verify the sanity of config options
// We don't have to verify the log level because the config_options module
// Sanitizes that to a sane value

// Also, any input option set to be *only* numeric/u64/f64 (ie, skew_window)
// Will always be a valid number because the Clap parser will die if the input is bad

use crate::Input;
use log::{error, trace};
use std::collections::HashSet;
use std::hash::Hash;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;

impl Input {
    /// Function for testing sanity of the configuration.
    ///
    /// This looks for things like whether provided ports are valid for use.
    /// It also makes sure that provided host:port addresses are properly formatted.
    /// Finally, it makes sure that there are no duplicate ports provided across configuration that takes port input.
    pub fn check_config_option_sanity(&self) -> Result<(), String> {
        // Create our mutable vector of bool. All check results will come into here.
        let is_input_sane: Vec<bool> = vec![
            // Make sure all provided ports are valid.
            self.check_port_validity(),
            // Make sure all provided hosts are valid.
            self.check_host_validity(),
            // Make sure that there are no duplicate ports between blocks.
            self.check_no_duplicate_ports(),
            // Make sure that there are no duplicate hosts between blocks.
            self.check_no_duplicate_hosts(),
        ];
        // Now, see if we have any failures, and how many of them. If there are any failures, throw back an `Err()`
        if is_input_sane.contains(&false) {
            Err(format!(
                "Config option sanity check failed, with {} total errors",
                is_input_sane.iter().filter(|&n| !(*n)).count()
            ))
        } else {
            Ok(())
        }
    }

    fn check_no_duplicate_ports(&self) -> bool {
        // We want to verify that no ports are duplicated, but only the ports that are going to be bound
        // AR_RECV, AR_SEND are not checked because we make outbound connections on those
        // AR_LISTEN and AR_SERVE are checked because we bind ports on the local machine to those

        let mut ports_udp: Vec<u16> = Vec::new();
        let mut ports_tcp_and_zmq: Vec<u16> = Vec::new();
        let mut input_test_results: Vec<bool> = Vec::new();

        let udp_sources: Vec<Option<&Vec<u16>>> = vec![
            self.listen_udp_acars.as_ref(),
            self.listen_udp_vdlm2.as_ref(),
            self.listen_udp_hfdl.as_ref(),
            self.listen_udp_imsl.as_ref(),
            self.listen_udp_irdm.as_ref(),
        ];
        let tcp_zmq_sources: Vec<Option<&Vec<u16>>> = vec![
            self.listen_tcp_acars.as_ref(),
            self.listen_tcp_vdlm2.as_ref(),
            self.listen_tcp_hfdl.as_ref(),
            self.listen_tcp_imsl.as_ref(),
            self.listen_tcp_irdm.as_ref(),
            self.listen_zmq_acars.as_ref(),
            self.listen_zmq_vdlm2.as_ref(),
            self.listen_zmq_hfdl.as_ref(),
            self.listen_zmq_imsl.as_ref(),
            self.listen_zmq_irdm.as_ref(),
            self.serve_tcp_acars.as_ref(),
            self.serve_tcp_vdlm2.as_ref(),
            self.serve_tcp_hfdl.as_ref(),
            self.serve_tcp_imsl.as_ref(),
            self.serve_tcp_irdm.as_ref(),
            self.serve_zmq_acars.as_ref(),
            self.serve_zmq_vdlm2.as_ref(),
            self.serve_zmq_hfdl.as_ref(),
            self.serve_zmq_imsl.as_ref(),
            self.serve_zmq_irdm.as_ref(),
        ];

        ports_udp.get_all_ports(&udp_sources);
        ports_tcp_and_zmq.get_all_ports(&tcp_zmq_sources);

        input_test_results.push(has_unique_elements(ports_udp));
        input_test_results.push(has_unique_elements(ports_tcp_and_zmq));

        input_test_results.validate_results()
    }

    fn check_no_duplicate_hosts(&self) -> bool {
        // We want to verify that no hosts are duplicated for an input
        // AR_RECV, AR_SEND are checked
        let config: Self = self.clone();
        let mut input_test_results: Vec<bool> = Vec::new();
        let mut all_hosts: Vec<String> = Vec::new();

        let check_entries = vec![
            self.receive_tcp_acars.as_ref(),
            self.receive_zmq_acars.as_ref(),
            self.receive_tcp_vdlm2.as_ref(),
            self.receive_zmq_vdlm2.as_ref(),
            self.receive_tcp_hfdl.as_ref(),
            self.receive_zmq_hfdl.as_ref(),
            self.receive_tcp_imsl.as_ref(),
            self.receive_zmq_imsl.as_ref(),
            self.receive_tcp_irdm.as_ref(),
            self.receive_zmq_irdm.as_ref(),
            self.send_tcp_acars.as_ref(),
            self.send_udp_acars.as_ref(),
            self.send_tcp_vdlm2.as_ref(),
            self.send_udp_vdlm2.as_ref(),
            self.send_tcp_hfdl.as_ref(),
            self.send_udp_hfdl.as_ref(),
            self.send_tcp_imsl.as_ref(),
            self.send_udp_imsl.as_ref(),
            self.send_tcp_irdm.as_ref(),
            self.send_udp_irdm.as_ref(),
        ];

        all_hosts.get_all_hosts(&check_entries);

        input_test_results.push(has_unique_elements(all_hosts));

        if let Some(config_hosts) = config.receive_tcp_acars {
            input_test_results
                .push(config_hosts.check_host("--receive-tcp-acars/AR_RECV_TCP_ACARS"));
        }
        if let Some(config_hosts) = config.receive_zmq_acars {
            input_test_results
                .push(config_hosts.check_host("--receive-zmq-acars/AR_RECEIVE_ZMQ_ACARS"));
        }
        if let Some(config_hosts) = config.receive_tcp_vdlm2 {
            input_test_results
                .push(config_hosts.check_host("--receive-tcp-vdlm2/AR_RECV_TCP_VDLM2"));
        }
        if let Some(config_hosts) = config.receive_zmq_vdlm2 {
            input_test_results
                .push(config_hosts.check_host("--receive-zmq-vdlm2/AR_RECEIVE_ZMQ_VDLM2"));
        }
        if let Some(config_hosts) = config.receive_tcp_hfdl {
            input_test_results.push(config_hosts.check_host("--receive-tcp-hfdl/AR_RECV_TCP_HFDL"));
        }
        if let Some(config_hosts) = config.receive_zmq_hfdl {
            input_test_results
                .push(config_hosts.check_host("--receive-zmq-hfdl/AR_RECEIVE_ZMQ_HFDL"));
        }
        if let Some(config_hosts) = config.receive_tcp_imsl {
            input_test_results.push(config_hosts.check_host("--receive-tcp-imsl/AR_RECV_TCP_IMSL"));
        }
        if let Some(config_hosts) = config.receive_zmq_imsl {
            input_test_results
                .push(config_hosts.check_host("--receive-zmq-imsl/AR_RECEIVE_ZMQ_IMSL"));
        }
        if let Some(config_hosts) = config.receive_tcp_irdm {
            input_test_results.push(config_hosts.check_host("--receive-tcp-irdm/AR_RECV_TCP_IRDM"));
        }
        if let Some(config_hosts) = config.receive_zmq_irdm {
            input_test_results
                .push(config_hosts.check_host("--receive-zmq-irdm/AR_RECEIVE_ZMQ_IRDM"));
        }
        if let Some(config_hosts) = config.send_tcp_acars {
            input_test_results.push(config_hosts.check_host("--send-tcp-acars/AR_SEND_TCP_ACARS"));
        }
        if let Some(config_hosts) = config.send_udp_acars {
            input_test_results.push(config_hosts.check_host("--send-udp-acars/AR_SEND_UDP_ACARS"));
        }
        if let Some(config_hosts) = config.send_tcp_vdlm2 {
            input_test_results.push(config_hosts.check_host("--send-tcp-vdlm2/AR_SEND_TCP_VDLM2"));
        }
        if let Some(config_hosts) = config.send_udp_vdlm2 {
            input_test_results.push(config_hosts.check_host("--send-udp-vdlm2/AR_SEND_UDP_VDLM2"));
        }
        if let Some(config_hosts) = config.send_tcp_hfdl {
            input_test_results.push(config_hosts.check_host("--send-tcp-hfdl/AR_SEND_TCP_HFDL"));
        }
        if let Some(config_hosts) = config.send_udp_hfdl {
            input_test_results.push(config_hosts.check_host("--send-udp-hfdl/AR_SEND_UDP_HFDL"));
        }
        if let Some(config_hosts) = config.send_tcp_imsl {
            input_test_results.push(config_hosts.check_host("--send-tcp-imsl/AR_SEND_TCP_IMSL"));
        }
        if let Some(config_hosts) = config.send_udp_imsl {
            input_test_results.push(config_hosts.check_host("--send-udp-imsl/AR_SEND_UDP_IMSL"));
        }
        if let Some(config_hosts) = config.send_tcp_irdm {
            input_test_results.push(config_hosts.check_host("--send-tcp-irdm/AR_SEND_TCP_IRDM"));
        }
        if let Some(config_hosts) = config.send_udp_irdm {
            input_test_results.push(config_hosts.check_host("--send-udp-irdm/AR_SEND_UDP_IRDM"));
        }

        input_test_results.validate_results()
    }

    fn check_port_validity(&self) -> bool {
        // Create our mutable vector of bool. All check results will come into here.
        let input_test_results: Vec<bool> = vec![
            // ACARS
            self.listen_udp_acars
                .check_ports_are_valid("AR_LISTEN_UDP_ACARS/--listen-udp-acars"),
            self.listen_tcp_acars
                .check_ports_are_valid("AR_LISTEN_TCP_ACARS/--listen-tcp-acars"),
            self.listen_zmq_acars
                .check_ports_are_valid("AR_LISTEN_ZMQ_ACARS/--listen-zmq-acars"),
            self.serve_tcp_acars
                .check_ports_are_valid("AR_SERVE_TCP_ACARS/--serve-tcp-acars"),
            self.serve_zmq_acars
                .check_ports_are_valid("AR_SERVE_ZMQ_ACARS/--serve-zmq-acars"),
            // VDLM2
            self.listen_udp_vdlm2
                .check_ports_are_valid("AR_LISTEN_UDP_VDLM2/--listen-udp-vdlm2"),
            self.listen_tcp_vdlm2
                .check_ports_are_valid("AR_LISTEN_TCP_VDLM2/--listen-tcp-vdlm2"),
            self.listen_zmq_vdlm2
                .check_ports_are_valid("AR_LISTEN_ZMQ_VDLM2/--listen-zmq-vdlm2"),
            self.serve_tcp_vdlm2
                .check_ports_are_valid("AR_SERVE_TCP_VDLM2/--serve-tcp-vdlm2"),
            self.serve_zmq_vdlm2
                .check_ports_are_valid("AR_SERVE_ZMQ_VDLM2/--serve-zmq-vdlm2"),
            // HFDL
            self.listen_udp_hfdl
                .check_ports_are_valid("AR_LISTEN_UDP_HFDL/--listen-udp-hfdl"),
            self.listen_tcp_hfdl
                .check_ports_are_valid("AR_LISTEN_TCP_HFDL/--listen-tcp-hfdl"),
            self.listen_zmq_hfdl
                .check_ports_are_valid("AR_LISTEN_ZMQ_HFDL/--listen-zmq-hfdl"),
            self.serve_tcp_hfdl
                .check_ports_are_valid("AR_SERVE_TCP_HFDL/--serve-tcp-hfdl"),
            self.serve_zmq_hfdl
                .check_ports_are_valid("AR_SERVE_ZMQ_HFDL/--serve-zmq-hfdl"),
            // IMSL
            self.listen_udp_imsl
                .check_ports_are_valid("AR_LISTEN_UDP_IMSL/--listen-udp-imsl"),
            self.listen_tcp_imsl
                .check_ports_are_valid("AR_LISTEN_TCP_IMSL/--listen-tcp-imsl"),
            self.listen_zmq_imsl
                .check_ports_are_valid("AR_LISTEN_ZMQ_IMSL/--listen-zmq-imsl"),
            self.serve_tcp_imsl
                .check_ports_are_valid("AR_SERVE_TCP_IMSL/--serve-tcp-imsl"),
            self.serve_zmq_imsl
                .check_ports_are_valid("AR_SERVE_ZMQ_IMSL/--serve-zmq-imsl"),
            // IRDM
            self.listen_udp_irdm
                .check_ports_are_valid("AR_LISTEN_UDP_IRDM/--listen-udp-irdm"),
            self.listen_tcp_irdm
                .check_ports_are_valid("AR_LISTEN_TCP_IRDM/--listen-tcp-irdm"),
            self.listen_zmq_irdm
                .check_ports_are_valid("AR_LISTEN_ZMQ_IRDM/--listen-zmq-irdm"),
            self.serve_tcp_irdm
                .check_ports_are_valid("AR_SERVE_TCP_IRDM/--serve-tcp-irdm"),
            self.serve_zmq_irdm
                .check_ports_are_valid("AR_SERVE_ZMQ_IRDM/--serve-zmq-irdm"),
        ];

        input_test_results.validate_results()
    }

    fn check_host_validity(&self) -> bool {
        // Create our mutable vector of bool. All check results will come into here.
        let input_test_results: Vec<bool> = vec![
            // ACARS
            self.receive_tcp_acars
                .check_host_is_valid("AR_RECEIVE_TCP_ACARS/--receive-tcp-acars"),
            self.receive_zmq_acars
                .check_host_is_valid("AR_RECEIVE_ZMQ_ACARS/--receive-zmq-acars"),
            self.send_udp_acars
                .check_host_is_valid("AR_SEND_UDP_ACARS/--send-udp-acars"),
            self.send_tcp_acars
                .check_host_is_valid("AR_SEND_TCP_ACARS/--send-tcp-acars"),
            // VDLM2
            self.receive_tcp_vdlm2
                .check_host_is_valid("AR_RECEIVE_TCP_VDLM2/--receive-tcp-vdlm2"),
            self.receive_zmq_vdlm2
                .check_host_is_valid("AR_RECEIVE_ZMQ_VDLM2/--receive-zmq-vdlm2"),
            self.send_udp_vdlm2
                .check_host_is_valid("AR_SEND_UDP_VDLM2/--send-udp-vdlm2"),
            self.send_tcp_vdlm2
                .check_host_is_valid("AR_SEND_TCP_VDLM2/--send-tcp-vdlm2"),
            // HFDL
            self.receive_tcp_hfdl
                .check_host_is_valid("AR_RECEIVE_TCP_HFDL/--receive-tcp-hfdl"),
            self.receive_zmq_hfdl
                .check_host_is_valid("AR_RECEIVE_ZMQ_HFDL/--receive-zmq-hfdl"),
            self.send_udp_hfdl
                .check_host_is_valid("AR_SEND_UDP_HFDL/--send-udp-hfdl"),
            self.send_tcp_hfdl
                .check_host_is_valid("AR_SEND_TCP_HFDL/--send-tcp-hfdl"),
            // IMSL
            self.receive_tcp_imsl
                .check_host_is_valid("AR_RECEIVE_TCP_IMSL/--receive-tcp-imsl"),
            self.receive_zmq_imsl
                .check_host_is_valid("AR_RECEIVE_ZMQ_IMSL/--receive-zmq-imsl"),
            self.send_udp_imsl
                .check_host_is_valid("AR_SEND_UDP_IMSL/--send-udp-imsl"),
            self.send_tcp_imsl
                .check_host_is_valid("AR_SEND_TCP_IMSL/--send-tcp-imsl"),
            // IRDM
            self.receive_tcp_irdm
                .check_host_is_valid("AR_RECEIVE_TCP_IRDM/--receive-tcp-irdm"),
            self.receive_zmq_irdm
                .check_host_is_valid("AR_RECEIVE_ZMQ_IRDM/--receive-zmq-irdm"),
            self.send_udp_irdm
                .check_host_is_valid("AR_SEND_UDP_IRDM/--send-udp-irdm"),
            self.send_tcp_irdm
                .check_host_is_valid("AR_SEND_TCP_IRDM/--send-tcp-irdm"),
        ];

        input_test_results.validate_results()
    }
}

fn has_unique_elements<T>(iter: T) -> bool
where
    T: IntoIterator,
    T::Item: Eq + Hash,
{
    let mut uniq = HashSet::new();
    iter.into_iter().all(move |x| uniq.insert(x))
}

trait PortDuplicateCheck {
    fn get_all_ports(&mut self, ports: &[Option<&Vec<u16>>]);
}

impl PortDuplicateCheck for Vec<u16> {
    fn get_all_ports(&mut self, ports_group: &[Option<&Vec<u16>>]) {
        let ports_group = ports_group.to_vec();
        for config_ports in ports_group.into_iter().flatten() {
            for port in config_ports {
                self.push(*port);
            }
        }
    }
}

trait HostDuplicateCheck {
    fn get_all_hosts(&mut self, hosts_group: &[Option<&Vec<String>>]);
    fn check_host(self, check_type: &str) -> bool;
}

impl HostDuplicateCheck for Vec<String> {
    fn get_all_hosts(&mut self, hosts_group: &[Option<&Vec<String>>]) {
        let hosts_group = hosts_group.to_vec();
        for config_hosts in hosts_group.into_iter().flatten() {
            for host in config_hosts {
                self.push(host.clone());
            }
        }
    }

    fn check_host(self, check_type: &str) -> bool {
        let unique_hosts = has_unique_elements(self.clone());
        if !unique_hosts {
            error!("Duplicate host in {check_type} configuration!\nContents: {self:?}");
        }
        unique_hosts
    }
}

/// Trait for processing check results
trait ReturnValidity {
    fn validate_results(self) -> bool;
}

/// Implementation of `ReturnValidity` for `Vec<bool>`.
///
/// It will check to see if there is an instance of `false` in the `Vec<bool>`.
/// If there is, it will return false, as we have a check that has failed.
impl ReturnValidity for Vec<bool> {
    fn validate_results(self) -> bool {
        // The logic here is "Did a test return false?"
        // If it did, the config has an error and the return should be false.
        !self.contains(&false)
    }
}

trait ValidatePorts {
    fn check_ports_are_valid(&self, name: &str) -> bool;
}

impl ValidatePorts for Option<Vec<u16>> {
    fn check_ports_are_valid(&self, name: &str) -> bool {
        // if we have a zero length vector the input is always bad
        if let Some(ports) = self {
            if ports.is_empty() {
                error!("{name} An empty listen ports vec has been provided");
                return false;
            }
            for port in ports {
                if port.eq(&0) {
                    error!("{name} Listen Port is invalid as it is zero");
                    return false;
                }
            }
        }
        true
    }
}

trait ValidateHosts {
    fn check_host_is_valid(&self, name: &str) -> bool;
}

impl ValidateHosts for Option<Vec<String>> {
    fn check_host_is_valid(&self, name: &str) -> bool {
        if let Some(sockets) = self {
            if sockets.is_empty() {
                error!("{name} has been provided, but there are no socket addresses");
                return false;
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
                        error!(
                            "{name}: Socket address is valid, but the port provided is zero: {socket}"
                        );
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

                // Validate the port.
                let Ok(port) = port_str.parse::<u16>() else {
                    error!("{name} Port is invalid for: {socket}");
                    return false;
                };
                if port == 0 {
                    error!(
                        "{name}: Socket address is valid, but the port provided is zero: {socket}"
                    );
                    return false;
                }

                // Validate the host portion. We intentionally do NOT perform DNS
                // resolution here; only cheap structural checks.
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

                // Belt-and-suspenders: if the host is bracketed it must contain
                // a valid IP literal inside. `SocketAddr::from_str` above should
                // already have accepted bracketed-IPv6 + port forms.
                if bracketed {
                    let stripped = &host[1..host.len() - 1];
                    if IpAddr::from_str(stripped).is_err() {
                        error!("{name} has an invalid bracketed host for: {socket}");
                        return false;
                    }
                }

                trace!("{socket} is a valid socket address");
            }
        }
        true
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_check_ports_are_valid_with_host() {
        let valid_hosts: Option<Vec<String>> = Some(vec![
            "127.0.0.1:8008".to_string(),
            "10.0.0.1:12345".to_string(),
            "192.168.1.1:65535".to_string(),
            "localhost:8008".to_string(),
            "ok.go:1234".to_string(),
        ]);
        let invalid_hosts: Option<Vec<String>> = Some(vec![
            "127.0.0.1:0".to_string(),
            "10.0.0.1".to_string(),
            "192.168.1.1:65536".to_string(),
            "localhost".to_string(),
            "alpha.go".to_string(),
            "localhost:65536".to_string(),
            "abc:12three".to_string(),
            "host:123:456".to_string(),
        ]);
        let empty_host_vec: Option<Vec<String>> = Some(vec![]);
        let valid_hosts_tests: bool = valid_hosts.check_host_is_valid("valid_hosts");
        let invalid_hosts_tests: bool = invalid_hosts.check_host_is_valid("invalid_hosts");
        let empty_host_vec_test: bool = empty_host_vec.check_host_is_valid("empty_vec");
        assert!(valid_hosts_tests);
        assert!(!invalid_hosts_tests);
        assert!(!empty_host_vec_test);
    }

    #[test]
    fn test_check_ports_are_valid() {
        let valid_ports: Option<Vec<u16>> = Some(vec![1, 8008, 65535]);
        let invalid_ports: Option<Vec<u16>> = Some(vec![0]);
        let empty_ports: Option<Vec<u16>> = Some(vec![]);
        let valid_ports_test: bool = valid_ports.check_ports_are_valid("valid_ports");
        let invalid_ports_test: bool = invalid_ports.check_ports_are_valid("invalid_ports");
        let empty_ports_test: bool = empty_ports.check_ports_are_valid("empty_ports");
        assert!(valid_ports_test);
        assert!(!invalid_ports_test);
        assert!(!empty_ports_test);
    }

    #[test]
    fn test_check_no_duplicate_ports() {
        let mut ports: Input = Input {
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

        let valid_ports_test: bool = ports.check_no_duplicate_ports();

        // Duplicate one set of ports
        ports.serve_zmq_vdlm2 = Some(vec![8008, 65528]);
        let invalid_ports_test: bool = ports.check_no_duplicate_ports();

        assert!(valid_ports_test, "Expected valid ports to pass");
        assert!(!invalid_ports_test, "Expected invalid ports to fail");

        // HFDL/ACARS cross-protocol TCP listen port collision.
        let hfdl_collision: Input = Input {
            listen_tcp_acars: Some(vec![8008]),
            listen_tcp_hfdl: Some(vec![8008]),
            ..Input::default()
        };
        assert!(
            !hfdl_collision.check_no_duplicate_ports(),
            "Expected HFDL/ACARS TCP collision to fail",
        );
    }

    #[test]
    fn test_check_no_duplicate_ports_imsl_irdm() {
        // IMSL listen_zmq collides with IRDM serve_zmq.
        let imsl_irdm_zmq: Input = Input {
            listen_zmq_imsl: Some(vec![9001]),
            serve_zmq_irdm: Some(vec![9001]),
            ..Input::default()
        };
        assert!(
            !imsl_irdm_zmq.check_no_duplicate_ports(),
            "Expected IMSL/IRDM ZMQ collision to fail",
        );

        // IMSL listen_udp collides with IRDM listen_udp.
        let imsl_irdm_udp: Input = Input {
            listen_udp_imsl: Some(vec![5550]),
            listen_udp_irdm: Some(vec![5550]),
            ..Input::default()
        };
        assert!(
            !imsl_irdm_udp.check_no_duplicate_ports(),
            "Expected IMSL/IRDM UDP collision to fail",
        );

        // Different ports across all protocols should pass.
        let no_collisions: Input = Input {
            listen_udp_imsl: Some(vec![5550]),
            listen_udp_irdm: Some(vec![5551]),
            serve_tcp_imsl: Some(vec![5552]),
            serve_tcp_irdm: Some(vec![5553]),
            listen_zmq_imsl: Some(vec![5554]),
            serve_zmq_irdm: Some(vec![5555]),
            ..Input::default()
        };
        assert!(
            no_collisions.check_no_duplicate_ports(),
            "Expected non-colliding ports to pass",
        );
    }

    #[test]
    fn test_check_host_is_valid_ipv6() {
        let valid: Option<Vec<String>> = Some(vec![
            "[::1]:8080".to_string(),
            "[2001:db8::1]:443".to_string(),
            "example.com:8080".to_string(),
        ]);
        assert!(valid.check_host_is_valid("valid_v6"));

        let cases_expected_invalid: &[&str] = &[
            "[::1]:0",
            "fe80::1",
            "[::1]",
            "example.com",
            ":8080",
            "example.com:0",
            "example.com:abc",
        ];
        for case in cases_expected_invalid {
            let v: Option<Vec<String>> = Some(vec![(*case).to_string()]);
            assert!(
                !v.check_host_is_valid("invalid_v6"),
                "expected {case} to be rejected",
            );
        }
    }

    #[test]
    fn test_check_no_duplicate_hosts() {
        let mut hosts: Input = Input {
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
            receive_tcp_hfdl: Some(vec![
                "test.com:8088".to_string(),
                "192.168.1.1:8088".to_string(),
            ]),
            receive_zmq_hfdl: Some(vec![
                "test.com:8089".to_string(),
                "192.168.1.1:8089".to_string(),
            ]),
            send_tcp_hfdl: Some(vec![
                "test.com:8090".to_string(),
                "192.168.1.1:8090".to_string(),
            ]),
            send_udp_hfdl: Some(vec![
                "test.com:8091".to_string(),
                "192.168.1.1:8091".to_string(),
            ]),
            receive_tcp_imsl: Some(vec![
                "test.com:8092".to_string(),
                "192.168.1.1:8092".to_string(),
            ]),
            receive_zmq_imsl: Some(vec![
                "test.com:8093".to_string(),
                "192.168.1.1:8093".to_string(),
            ]),
            send_tcp_imsl: Some(vec![
                "test.com:8094".to_string(),
                "192.168.1.1:8094".to_string(),
            ]),
            send_udp_imsl: Some(vec![
                "test.com:8095".to_string(),
                "192.168.1.1:8095".to_string(),
            ]),
            receive_tcp_irdm: Some(vec![
                "test.com:8096".to_string(),
                "192.168.1.1:8096".to_string(),
            ]),
            receive_zmq_irdm: Some(vec![
                "test.com:8097".to_string(),
                "192.168.1.1:8097".to_string(),
            ]),
            send_tcp_irdm: Some(vec![
                "test.com:8098".to_string(),
                "192.168.1.1:8098".to_string(),
            ]),
            send_udp_irdm: Some(vec![
                "test.com:8099".to_string(),
                "192.168.1.1:8099".to_string(),
            ]),
            ..Input::default()
        };
        let valid_hosts_test: bool = hosts.check_no_duplicate_hosts();

        hosts.send_udp_vdlm2 = Some(vec![
            "test.com:8087".to_string(),
            "192.168.1.1:8087".to_string(),
            "test.com:8087".to_string(),
        ]);
        let invalid_hosts_test: bool = hosts.check_no_duplicate_hosts();

        assert!(
            valid_hosts_test,
            "Expected there to be no duplicates for this check"
        );
        assert!(
            !invalid_hosts_test,
            "Expected there to be duplicates for this check"
        );
    }
}
