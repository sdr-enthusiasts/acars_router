// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

// File to verify the sanity of config options

use crate::config_options::Input;
use std::net::SocketAddr;
use std::str::FromStr;

pub fn check_config_option_sanity(config_options: &Input) -> Result<(), String> {
    let mut is_input_sane = true;

    // We don't have to verify the log level because the config_options module
    // Sanitizes that to a sane value

    // Also, any input option set to be *only* numeric/u64 (ie, skew_window)
    // Will always be a valid number because the Clap parser will die if the input is bad

    if !check_ports_are_valid(
        &config_options.listen_udp_acars,
        "AR_LISTEN_UDP_ACARS/--listen-udp-acars",
    ) {
        is_input_sane = false;
    }

    if !check_ports_are_valid(
        &config_options.listen_tcp_acars,
        "AR_LISTEN_TCP_ACARS/--listen-tcp-acars",
    ) {
        is_input_sane = false;
    }

    if !check_ports_are_valid_with_host(
        &config_options.receive_tcp_acars,
        "AR_RECEIVE_TCP_ACARS/--receive-tcp-acars",
    ) {
        is_input_sane = false;
    }

    if !check_ports_are_valid(
        &config_options.listen_udp_vdlm2,
        "AR_LISTEN_UDP_VDLM2/--listen-udp-vdlm2",
    ) {
        is_input_sane = false;
    }

    if !check_ports_are_valid(
        &config_options.listen_tcp_vdlm2,
        "AR_LISTEN_TCP_VDLM2/--listen-tcp-vdlm2",
    ) {
        is_input_sane = false;
    }

    if !check_ports_are_valid_with_host(
        &config_options.receive_tcp_vdlm2,
        "AR_RECEIVE_TCP_VDLM2/--receive-tcp-vdlm2",
    ) {
        is_input_sane = false;
    }

    if !check_ports_are_valid_with_host(
        &config_options.send_udp_acars,
        "AR_SEND_UDP_ACARS/--send-udp-acars",
    ) {
        is_input_sane = false;
    }

    if !check_ports_are_valid_with_host(
        &config_options.send_udp_vdlm2,
        "AR_SEND_UDP_VDLM2/--send-udp-vdlm2",
    ) {
        is_input_sane = false;
    }

    if !check_ports_are_valid_with_host(
        &config_options.send_tcp_acars,
        "AR_SEND_TCP_ACARS/--send-tcp-acars",
    ) {
        is_input_sane = false;
    }

    if !check_ports_are_valid_with_host(
        &config_options.send_tcp_vdlm2,
        "AR_SEND_TCP_VDLM2/--send-tcp-vdlm2",
    ) {
        is_input_sane = false;
    }

    if !check_ports_are_valid_with_host(
        &config_options.receive_zmq_vdlm2,
        "AR_RECEIVE_ZMQ_VDLM2/--receive-zmq-vdlm2",
    ) {
        is_input_sane = false;
    }

    if !check_ports_are_valid_with_host(
        &config_options.receive_zmq_acars,
        "AR_RECEIVE_ZMQ_ACARS/--receive-zmq-acars",
    ) {
        is_input_sane = false;
    }

    if !check_ports_are_valid(
        &config_options.serve_tcp_acars,
        "AR_SERVE_TCP_ACARS/--serve-tcp-acars",
    ) {
        is_input_sane = false;
    }

    if !check_ports_are_valid(
        &config_options.serve_tcp_vdlm2,
        "AR_SERVE_TCP_VDLM2/--serve-tcp-vdlm2",
    ) {
        is_input_sane = false;
    }

    if !check_ports_are_valid(
        &config_options.serve_zmq_acars,
        "AR_SERVE_ZMQ_ACARS/--serve-zmq-acars",
    ) {
        is_input_sane = false;
    }

    if !check_ports_are_valid(
        &config_options.serve_zmq_vdlm2,
        "AR_SERVE_ZMQ_VDLM2/--serve-zmq-vdlm2",
    ) {
        is_input_sane = false;
    }

    if !check_no_duplicate_ports(config_options) {
        is_input_sane = false;
    }

    if !check_no_duplicate_hosts(config_options) {
        is_input_sane = false;
    }

    match is_input_sane {
        true => Ok(()),
        false => Err("Config option sanity check failed".to_string()),
    }
}

fn check_no_duplicate_ports(config: &Input) -> bool {
    // We want to verify that no ports are duplicated, but only the ports that are going to be bound
    // AR_RECV, AR_SEND are not checked because we make outbound connections on those
    // AR_LISTEN and AR_SERVE are checked because we bind ports on the local machine to those

    let mut ports_udp: Vec<u16> = Vec::new();
    let mut ports_tcp_and_zmq: Vec<u16> = Vec::new();
    let mut is_input_sane = true;

    if let Some(ports_in_config) = &config.listen_udp_acars {
        for port in ports_in_config {
            if ports_udp.contains(&port) {
                is_input_sane = false;
                error!("Duplicate port {} in configuration!", port);
            } else {
                ports_udp.push(port.clone());
            }
        }
    }

    if let Some(ports_in_config) = &config.listen_tcp_acars {
        for port in ports_in_config {
            if ports_tcp_and_zmq.contains(&port) {
                is_input_sane = false;
                error!("Duplicate port {} in configuration!", port);
            } else {
                ports_tcp_and_zmq.push(port.clone());
            }
        }
    }

    if let Some(ports_in_config) = &config.listen_udp_vdlm2 {
        for port in ports_in_config {
            if ports_udp.contains(&port) {
                is_input_sane = false;
                error!("Duplicate port {} in configuration!", port);
            } else {
                ports_udp.push(port.clone());
            }
        }
    }

    if let Some(ports_in_config) = &config.listen_tcp_vdlm2 {
        for port in ports_in_config {
            if ports_tcp_and_zmq.contains(&port) {
                is_input_sane = false;
                error!("Duplicate port {} in configuration!", port);
            } else {
                ports_tcp_and_zmq.push(port.clone());
            }
        }
    }

    if let Some(ports_in_config) = &config.serve_tcp_acars {
        for port in ports_in_config {
            if ports_tcp_and_zmq.contains(&port) {
                is_input_sane = false;
                error!("Duplicate port {} in configuration!", port);
            } else {
                ports_tcp_and_zmq.push(port.clone());
            }
        }
    }

    if let Some(ports_in_config) = &config.serve_zmq_acars {
        for port in ports_in_config {
            if ports_tcp_and_zmq.contains(&port) {
                is_input_sane = false;
                error!("Duplicate port {} in configuration!", port);
            } else {
                ports_tcp_and_zmq.push(port.clone());
            }
        }
    }

    if let Some(ports_in_config) = &config.serve_tcp_vdlm2 {
        for port in ports_in_config {
            if ports_tcp_and_zmq.contains(&port) {
                is_input_sane = false;
                error!("Duplicate port {} in configuration!", port);
            } else {
                ports_tcp_and_zmq.push(port.clone());
            }
        }
    }

    if let Some(ports_in_config) = &config.serve_zmq_vdlm2 {
        for port in ports_in_config {
            if ports_tcp_and_zmq.contains(&port) {
                is_input_sane = false;
                error!("Duplicate port {} in configuration!", port);
            } else {
                ports_tcp_and_zmq.push(port.clone());
            }
        }
    }

    is_input_sane
}

fn check_no_duplicate_hosts(config: &Input) -> bool {
    // We want to verify that no hosts are duplicated for an input
    // AR_RECV, AR_SEND are checked
    let mut is_input_sane = true;
    let mut hosts: Vec<String> = Vec::new();
    if let Some(ports_in_config) = &config.receive_tcp_acars {
        for host in ports_in_config {
            if hosts.contains(&host) {
                is_input_sane = false;
                error!(
                    "Duplicate host {} in --receive-tcp-acars/AR_RECV_TCP_ACARS configuration!",
                    host
                );
            } else {
                hosts.push(host.clone());
            }
        }
    }
    hosts.clear();
    if let Some(ports_in_config) = &config.receive_zmq_acars {
        for host in ports_in_config {
            if hosts.contains(&host) {
                is_input_sane = false;
                error!(
                    "Duplicate host {} in --receive-zmq-acars/AR_RECEIVE_ZMQ_ACARS configuration!",
                    host
                );
            } else {
                hosts.push(host.clone());
            }
        }
    }
    hosts.clear();
    if let Some(ports_in_config) = &config.receive_tcp_vdlm2 {
        for host in ports_in_config {
            if hosts.contains(&host) {
                is_input_sane = false;
                error!(
                    "Duplicate host {} in --receive-tcp-vdlm2/AR_RECV_TCP_VDLM2 configuration!",
                    host
                );
            } else {
                hosts.push(host.clone());
            }
        }
    }
    hosts.clear();
    if let Some(ports_in_config) = &config.receive_zmq_vdlm2 {
        for host in ports_in_config {
            if hosts.contains(&host) {
                is_input_sane = false;
                error!(
                    "Duplicate host {} in --receive-zmq-vdlm2/AR_RECEIVE_ZMQ_VDLM2 configuration!",
                    host
                );
            } else {
                hosts.push(host.clone());
            }
        }
    }
    hosts.clear();
    if let Some(ports_in_config) = &config.send_tcp_acars {
        for host in ports_in_config {
            if hosts.contains(&host) {
                is_input_sane = false;
                error!(
                    "Duplicate host {} in --send-tcp-acars/AR_SEND_TCP_ACARS configuration!",
                    host
                );
            } else {
                hosts.push(host.clone());
            }
        }
    }
    hosts.clear();
    if let Some(ports_in_config) = &config.send_udp_acars {
        for host in ports_in_config {
            if hosts.contains(&host) {
                is_input_sane = false;
                error!(
                    "Duplicate host {} in --send-udp-acars/AR_SEND_UDP_ACARS configuration!",
                    host
                );
            } else {
                hosts.push(host.clone());
            }
        }
    }
    hosts.clear();
    if let Some(ports_in_config) = &config.send_tcp_vdlm2 {
        for host in ports_in_config {
            if hosts.contains(&host) {
                is_input_sane = false;
                error!(
                    "Duplicate host {} in --send-tcp-vdlm2/AR_SEND_TCP_VDLM2 configuration!",
                    host
                );
            } else {
                hosts.push(host.clone());
            }
        }
    }
    hosts.clear();
    if let Some(ports_in_config) = &config.send_udp_vdlm2 {
        for host in ports_in_config {
            if hosts.contains(&host) {
                is_input_sane = false;
                error!(
                    "Duplicate host {} in --send-udp-vdlm2/AR_SEND_UDP_VDLM2 configuration!",
                    host
                );
            } else {
                hosts.push(host.clone());
            }
        }
    }

    is_input_sane
}

fn check_ports_are_valid(option_ports: &Option<Vec<u16>>, name: &str) -> bool {
    let mut is_input_sane = true;
    // if we have a zero length vector the input is always bad
    if let Some(ports) = option_ports {
        if ports.is_empty() {
            error!("{} An empty listen ports vec has been provided", name);
            is_input_sane = false;
        }
        for port in ports {
            if port.eq(&0) {
                error!("{} Listen Port is invalid as it is zero", name);
                is_input_sane = false
            }
        }
    }
    is_input_sane
}

fn check_ports_are_valid_with_host(socket_addresses: &Option<Vec<String>>, name: &str) -> bool {
    let mut is_input_sane = true;

    if let Some(sockets) = socket_addresses {
        if sockets.is_empty() {
            error!(
                "{} has been provided, but there are no socket addresses",
                name
            );
            is_input_sane = false;
        }
        for socket in sockets {
            // FIXME: Just doing a socketaddr parse fails if the host name isn't an IP address.

            // check and see if there are alpha characters in the string
            if socket
                .chars()
                .any(|c| !c.is_numeric() && !c.eq(&'.') && !c.eq(&':'))
            {
                // split the string on ':'
                let socket_parts = socket.split(":").collect::<Vec<_>>();

                if socket_parts.len() != 2 {
                    is_input_sane = false;
                    error!(
                        "{} has been provided, but is not a valid socket address",
                        name
                    );
                    continue;
                }

                let port = socket_parts[1];
                // validate the port is numeric and between 1-65535
                if !port.chars().all(|c| c.is_numeric()) {
                    error!("{} Port is not numeric", name);
                    is_input_sane = false;
                } else {
                    match port.parse::<u16>() {
                        Ok(parsed_socket) => {
                            if parsed_socket == 0 {
                                error!(
                                    "{}: Socket address is valid, but the port provided is zero!",
                                    name
                                );
                                is_input_sane = false;
                            } else {
                                trace!("{} is a valid socket address", socket);
                            }
                        }
                        Err(_) => {
                            error!("{} Port is not numeric", name);
                            is_input_sane = false;
                        }
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
                        is_input_sane = false;
                    }
                    Ok(parsed_socket) => {
                        if parsed_socket.port().eq(&0) {
                            error!(
                                "{}: Socket address is valid, but the port provided is zero!",
                                name
                            );
                            is_input_sane = false;
                        } else {
                            trace!("{} is a valid socket address", socket);
                        }
                    }
                }
            }
        }
    }
    is_input_sane
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::config_options::Input;
    use clap::Parser;

    #[test]
    fn test_check_ports_are_valid_with_host() {
        let valid_hosts: Option<Vec<String>> = Some(vec![
            "127.0.0.1:8008".to_string(),
            "10.0.0.1:12345".to_string(),
            "192.168.1.1:65535".to_string(),
            "test.com:80".to_string(),
        ]);

        let invalid_hosts: Option<Vec<String>> = Some(vec![
            "127.0.0.1:0".to_string(),
            "10.0.0.1".to_string(),
            "192.168.1.1:65536".to_string(),
        ]);

        let empty_host_vec: Option<Vec<String>> = Some(vec![]);
        let valid_hosts_tests: bool = check_ports_are_valid_with_host(&valid_hosts, "valid_hosts");
        let invalid_hosts_tests: bool =
            check_ports_are_valid_with_host(&invalid_hosts, "invalid_hosts");
        let empty_host_vec_test: bool =
            check_ports_are_valid_with_host(&empty_host_vec, "empty_vec");

        assert_eq!(valid_hosts_tests, true);
        assert_eq!(invalid_hosts_tests, false);
        assert_eq!(empty_host_vec_test, false);
    }

    #[test]
    fn test_check_ports_are_valid() {
        let valid_ports: Option<Vec<u16>> = Some(vec![1, 8008, 65535]);
        let invalid_ports: Option<Vec<u16>> = Some(vec![0]);
        let empty_ports: Option<Vec<u16>> = Some(vec![]);

        let valid_ports_test: bool = check_ports_are_valid(&valid_ports, "valid_ports");
        let invalid_ports_test: bool = check_ports_are_valid(&invalid_ports, "invalid_ports");
        let empty_ports_test: bool = check_ports_are_valid(&empty_ports, "empty_ports");

        assert_eq!(valid_ports_test, true);
        assert_eq!(invalid_ports_test, false);
        assert_eq!(empty_ports_test, false);
    }

    #[test]
    fn test_duplicate_ports_are_valid() {
        let mut ports: Input = Input::parse();
        // Generate clean input
        ports.listen_udp_vdlm2 = Some(vec![8008, 65535]);
        ports.listen_udp_acars = Some(vec![8009, 65534]);
        ports.listen_tcp_acars = Some(vec![8008, 65533]);
        ports.listen_tcp_vdlm2 = Some(vec![8011, 65532]);
        ports.serve_tcp_acars = Some(vec![8012, 65531]);
        ports.serve_tcp_vdlm2 = Some(vec![8013, 65530]);
        ports.serve_zmq_acars = Some(vec![8014, 65529]);
        ports.serve_zmq_vdlm2 = Some(vec![8015, 65528]);

        let valid_ports_test: bool = check_no_duplicate_ports(&ports);

        // Duplicate one set of ports
        ports.serve_zmq_vdlm2 = Some(vec![8008, 65528]);
        let invalid_ports_test: bool = check_no_duplicate_ports(&ports);

        assert_eq!(valid_ports_test, true);
        assert_eq!(invalid_ports_test, false);
    }

    #[test]
    fn test_duplicate_hosts_are_valid() {
        let mut hosts: Input = Input::parse();
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
        let valid_hosts_test: bool = check_no_duplicate_hosts(&hosts);

        hosts.send_udp_vdlm2 = Some(vec![
            "test.com:8087".to_string(),
            "192.168.1.1:8087".to_string(),
            "test.com:8087".to_string(),
        ]);
        let invalid_hosts_test: bool = check_no_duplicate_hosts(&hosts);

        assert_eq!(valid_hosts_test, true);
        assert_eq!(invalid_hosts_test, false);
    }
}
