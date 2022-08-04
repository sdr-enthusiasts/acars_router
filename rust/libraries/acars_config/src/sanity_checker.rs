// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

// File to verify the sanity of config options

use crate::Input;
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
    
    match is_input_sane {
        true => Ok(()),
        false => Err("Config option sanity check failed".to_string()),
    }
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
            error!("{} has been provided, but there are no socket addresses",name);
            is_input_sane = false;
        }
        for socket in sockets {
            // check and see if there are alpha characters in the string
            if socket.chars().any(|c| c.is_alphabetic()) { // !c.is_numeric() && !c.eq(&'.') && !c.eq(&':')) {
                // split the string on ':'
                let socket_parts = socket.split(':').collect::<Vec<_>>();
                match socket_parts.len() {
                    1 => {
                        error!("{} has no port specified for: {}", name, socket);
                        is_input_sane = false;
                    },
                    2 => {
                        let port = socket_parts[1];
                        // validate the port is numeric and between 1-65535
                        if !port.chars().all(|c| c.is_numeric()) {
                            error!("{} Port is not numeric for: {}", name, socket);
                            is_input_sane = false;
                        } else {
                            match port.parse::<u16>() {
                                Ok(parsed_socket) => {
                                    if parsed_socket == 0 {
                                        error!("{}: Socket address is valid, but the port provided is zero: {}", name, socket);
                                        is_input_sane = false;
                                    } else {
                                        trace!("{} is a valid socket address", socket);
                                    }
                                }
                                Err(_) => {
                                    error!("{} Port is invalid for: {}", name, socket);
                                    is_input_sane = false;
                                }
                            }
                        }
                    },
                    3 | _ => {
                        error!("{} has an address with more than one colon in it: {}", name, socket);
                        is_input_sane = false;
                    }
                }
            } else {
                let parse_socket = SocketAddr::from_str(socket);
                match parse_socket {
                    Err(parse_error) => {
                        error!("{}: Failed to validate that {} is a properly formatted socket: {}", name, socket, parse_error);
                        is_input_sane = false;
                    }
                    Ok(parsed_socket) => {
                        if parsed_socket.port().eq(&0) {
                            error!("{}: Socket address is valid, but the port provided is zero: {}", name, socket);
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
    
    #[test]
    fn test_check_ports_are_valid_with_host() {
        let valid_hosts: Option<Vec<String>> = Some(vec![
            "127.0.0.1:8008".to_string(),
            "10.0.0.1:12345".to_string(),
            "192.168.1.1:65535".to_string(),
            "localhost:8008".to_string()
        ]);
        let invalid_hosts: Option<Vec<String>> = Some(vec![
            "127.0.0.1:0".to_string(),
            "10.0.0.1".to_string(),
            "192.168.1.1:65536".to_string(),
            "localhost".to_string(),
            "localhost:65536".to_string(),
            "123:456".to_string(),
            "abc:12three".to_string()
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
}
