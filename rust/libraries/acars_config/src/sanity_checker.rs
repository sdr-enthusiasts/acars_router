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
use std::collections::HashSet;
use std::hash::Hash;
use std::net::SocketAddr;
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
        match is_input_sane.contains(&false) {
            true => Err(format!(
                "Config option sanity check failed, with {} total errors",
                is_input_sane.iter().filter(|&n| !(*n)).count()
            )),
            false => Ok(()),
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
        ];
        let tcp_zmq_sources: Vec<Option<&Vec<u16>>> = vec![
            self.listen_tcp_acars.as_ref(),
            self.listen_tcp_vdlm2.as_ref(),
            self.serve_tcp_acars.as_ref(),
            self.serve_tcp_vdlm2.as_ref(),
            self.serve_zmq_acars.as_ref(),
            self.serve_zmq_vdlm2.as_ref(),
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
        let config: Input = self.clone();
        let mut input_test_results: Vec<bool> = Vec::new();
        let mut all_hosts: Vec<String> = Vec::new();

        let check_entries = vec![
            self.receive_tcp_acars.as_ref(),
            self.receive_zmq_acars.as_ref(),
            self.receive_tcp_vdlm2.as_ref(),
            self.receive_zmq_vdlm2.as_ref(),
            self.send_tcp_acars.as_ref(),
            self.send_udp_acars.as_ref(),
            self.send_tcp_vdlm2.as_ref(),
            self.send_udp_vdlm2.as_ref(),
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

        input_test_results.validate_results()
    }

    fn check_port_validity(&self) -> bool {
        // Create our mutable vector of bool. All check results will come into here.
        let input_test_results: Vec<bool> = vec![
            // Make sure the ports for AR_LISTEN_UDP_ACARS are valid.
            self.listen_udp_acars
                .check_ports_are_valid("AR_LISTEN_UDP_ACARS/--listen-udp-acars"),
            // Make sure that the ports for AR_LISTEN_TCP_ACARS are valid.
            self.listen_tcp_acars
                .check_ports_are_valid("AR_LISTEN_TCP_ACARS/--listen-tcp-acars"),
            // Make sure that the ports for AR_LISTEN_UDP_VDLM2 are valid.
            self.listen_udp_vdlm2
                .check_ports_are_valid("AR_LISTEN_UDP_VDLM2/--listen-udp-vdlm2"),
            // Make sure that the ports for AR_LISTEN_TCP_VDLM2 are valid.
            self.listen_tcp_vdlm2
                .check_ports_are_valid("AR_LISTEN_TCP_VDLM2/--listen-tcp-vdlm2"),
            // Make sure that the ports for AR_SERVE_TCP_ACARS are valid.
            self.serve_tcp_acars
                .check_ports_are_valid("AR_SERVE_TCP_ACARS/--serve-tcp-acars"),
            // Make sure that the ports for AR_SERVE_TCP_VDLM2 are valid.
            self.serve_tcp_vdlm2
                .check_ports_are_valid("AR_SERVE_TCP_VDLM2/--serve-tcp-vdlm2"),
            // Make sure that the ports for AR_SERVE_ZMQ_ACARS are valid.
            self.serve_zmq_acars
                .check_ports_are_valid("AR_SERVE_ZMQ_ACARS/--serve-zmq-acars"),
            // Make sure that the ports for AR_SERVE_ZMQ_VDLM2 are valid.
            self.serve_zmq_vdlm2
                .check_ports_are_valid("AR_SERVE_ZMQ_VDLM2/--serve-zmq-vdlm2"),
        ];

        input_test_results.validate_results()
    }

    fn check_host_validity(&self) -> bool {
        // Create our mutable vector of bool. All check results will come into here.
        let input_test_results: Vec<bool> = vec![
            // Make sure that the provided hosts for AR_RECEIVE_TCP_ACARS are valid.
            self.receive_tcp_acars
                .check_host_is_valid("AR_RECEIVE_TCP_ACARS/--receive-tcp-acars"),
            // Make sure that the provided hosts for AR_RECEIVE_TCP_VDLM2 are valid.
            self.receive_tcp_vdlm2
                .check_host_is_valid("AR_RECEIVE_TCP_VDLM2/--receive-tcp-vdlm2"),
            // Make sure that the provided hosts for AR_SEND_UDP_ACARS are valid.
            self.send_udp_acars
                .check_host_is_valid("AR_SEND_UDP_ACARS/--send-udp-acars"),
            // Make sure that the provided hosts for AR_SEND_UDP_VDLM2 are valid.
            self.send_udp_vdlm2
                .check_host_is_valid("AR_SEND_UDP_VDLM2/--send-udp-vdlm2"),
            // Make sure that the provided hosts for AR_SEND_TCP_ACARS are valid.
            self.send_tcp_acars
                .check_host_is_valid("AR_SEND_TCP_ACARS/--send-tcp-acars"),
            // Make sure that the provided hosts for AR_SEND_TCP_VDLM2 are valid.
            self.send_tcp_vdlm2
                .check_host_is_valid("AR_SEND_TCP_VDLM2/--send-tcp-vdlm2"),
            // Make sure that the provided hosts for AR_RECEIVE_ZMQ_VDLM2 are valid.
            self.receive_zmq_vdlm2
                .check_host_is_valid("AR_RECEIVE_ZMQ_VDLM2/--receive-zmq-vdlm2"),
            // Make sure that the provided hosts for AR_RECEIVE_ZMQ_ACARS are valid.
            self.receive_zmq_acars
                .check_host_is_valid("AR_RECEIVE_ZMQ_ACARS/--receive-zmq-acars"),
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
                self.push(host.to_string());
            }
        }
    }

    fn check_host(self, check_type: &str) -> bool {
        let unique_hosts = has_unique_elements(self.to_vec());
        if !unique_hosts {
            error!(
                "Duplicate host in {} configuration!\nContents: {:?}",
                check_type, self
            );
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
        match self.contains(&false) {
            // The logic here is "Did a test return false?"
            // If it did, the config has an error and the return should be false.
            true => false,
            false => true,
        }
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
                error!("{} An empty listen ports vec has been provided", name);
                return false;
            }
            for port in ports {
                if port.eq(&0) {
                    error!("{} Listen Port is invalid as it is zero", name);
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
                error!(
                    "{} has been provided, but there are no socket addresses",
                    name
                );
                return false;
            }
            for socket in sockets {
                // check and see if there are alpha characters in the string
                if socket.chars().any(|c| c.is_alphabetic()) {
                    // split the string on ':'
                    let socket_parts = socket.split(':').collect::<Vec<_>>();
                    match socket_parts.len() {
                        1 => {
                            error!("{} has no port specified for: {}", name, socket);
                            return false;
                        }
                        2 => {
                            let port = socket_parts[1];
                            // validate the port is numeric and between 1-65535
                            if !port.chars().all(|c| c.is_numeric()) {
                                error!("{} Port is not numeric for: {}", name, socket);
                                return false;
                            } else {
                                match port.parse::<u16>() {
                                    Ok(parsed_socket) => {
                                        if parsed_socket == 0 {
                                            error!("{}: Socket address is valid, but the port provided is zero: {}", name, socket);
                                            return false;
                                        } else {
                                            trace!("{} is a valid socket address", socket);
                                        }
                                    }
                                    Err(_) => {
                                        error!("{} Port is invalid for: {}", name, socket);
                                        return false;
                                    }
                                }
                            }
                        }
                        _ => {
                            error!(
                                "{} has an address with more than one colon in it: {}",
                                name, socket
                            );
                            return false;
                        }
                    }
                } else {
                    let parse_socket = SocketAddr::from_str(socket);
                    match parse_socket {
                        Err(parse_error) => {
                            error!(
                                "{}: Failed to validate that {} is a properly formatted socket: {}",
                                name, socket, parse_error
                            );
                            return false;
                        }
                        Ok(parsed_socket) => {
                            if parsed_socket.port().eq(&0) {
                                error!("{}: Socket address is valid, but the port provided is zero: {}", name, socket);
                                return false;
                            } else {
                                trace!("{} is a valid socket address", socket);
                            }
                        }
                    }
                }
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
            "123:456".to_string(),
            "abc:12three".to_string(),
            "host:123:456".to_string(),
        ]);
        let empty_host_vec: Option<Vec<String>> = Some(vec![]);
        let valid_hosts_tests: bool = valid_hosts.check_host_is_valid("valid_hosts");
        let invalid_hosts_tests: bool = invalid_hosts.check_host_is_valid("invalid_hosts");
        let empty_host_vec_test: bool = empty_host_vec.check_host_is_valid("empty_vec");
        assert_eq!(valid_hosts_tests, true);
        assert_eq!(invalid_hosts_tests, false);
        assert_eq!(empty_host_vec_test, false);
    }

    #[test]
    fn test_check_ports_are_valid() {
        let valid_ports: Option<Vec<u16>> = Some(vec![1, 8008, 65535]);
        let invalid_ports: Option<Vec<u16>> = Some(vec![0]);
        let empty_ports: Option<Vec<u16>> = Some(vec![]);
        let valid_ports_test: bool = valid_ports.check_ports_are_valid("valid_ports");
        let invalid_ports_test: bool = invalid_ports.check_ports_are_valid("invalid_ports");
        let empty_ports_test: bool = empty_ports.check_ports_are_valid("empty_ports");
        assert_eq!(valid_ports_test, true);
        assert_eq!(invalid_ports_test, false);
        assert_eq!(empty_ports_test, false);
    }

    #[test]
    fn test_check_no_duplicate_ports() {
        let mut ports: Input = Input::default();
        // Generate clean input
        ports.listen_udp_vdlm2 = Some(vec![8008, 65535]);
        ports.listen_udp_acars = Some(vec![8009, 65534]);
        ports.listen_tcp_acars = Some(vec![8008, 65533]);
        ports.listen_tcp_vdlm2 = Some(vec![8011, 65532]);
        ports.serve_tcp_acars = Some(vec![8012, 65531]);
        ports.serve_tcp_vdlm2 = Some(vec![8013, 65530]);
        ports.serve_zmq_acars = Some(vec![8014, 65529]);
        ports.serve_zmq_vdlm2 = Some(vec![8015, 65528]);

        let valid_ports_test: bool = ports.check_no_duplicate_ports();

        // Duplicate one set of ports
        ports.serve_zmq_vdlm2 = Some(vec![8008, 65528]);
        let invalid_ports_test: bool = ports.check_no_duplicate_ports();

        assert_eq!(valid_ports_test, true, "Expected valid ports to pass");
        assert_eq!(invalid_ports_test, false, "Expected invalid ports to fail");
    }

    #[test]
    fn test_check_no_duplicate_hosts() {
        let mut hosts: Input = Input::default();
        // Generate clean input
        hosts.receive_tcp_acars = Some(vec![
            "test.com:8080".to_string(),
            "192.168.1.1:8080".to_string(),
        ]);
        hosts.receive_zmq_acars = Some(vec![
            "test.com:8081".to_string(),
            "192.168.1.1:8081".to_string(),
        ]);
        hosts.receive_tcp_vdlm2 = Some(vec![
            "test.com:8082".to_string(),
            "192.168.1.1:8082".to_string(),
        ]);
        hosts.receive_zmq_vdlm2 = Some(vec![
            "test.com:8083".to_string(),
            "192.168.1.1:8083".to_string(),
        ]);
        hosts.send_tcp_acars = Some(vec![
            "test.com:8084".to_string(),
            "192.168.1.1:8084".to_string(),
        ]);
        hosts.send_udp_acars = Some(vec![
            "test.com:8085".to_string(),
            "192.168.1.1:8085".to_string(),
        ]);
        hosts.send_tcp_vdlm2 = Some(vec![
            "test.com:8086".to_string(),
            "192.168.1.1:8086".to_string(),
        ]);
        hosts.send_udp_vdlm2 = Some(vec![
            "test.com:8087".to_string(),
            "192.168.1.1:8087".to_string(),
        ]);
        let valid_hosts_test: bool = hosts.check_no_duplicate_hosts();

        hosts.send_udp_vdlm2 = Some(vec![
            "test.com:8087".to_string(),
            "192.168.1.1:8087".to_string(),
            "test.com:8087".to_string(),
        ]);
        let invalid_hosts_test: bool = hosts.check_no_duplicate_hosts();

        assert_eq!(
            valid_hosts_test, true,
            "Expected there to be no duplicates for this check"
        );
        assert_eq!(
            invalid_hosts_test, false,
            "Expected there to be duplicates for this check"
        );
    }
}
