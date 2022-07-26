// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

// File to verify the sanity of config options

use crate::config_options::ACARSRouterSettings;
use log::{error, trace};

pub fn check_config_option_sanity(config_options: &ACARSRouterSettings) -> Result<(), String> {
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

fn check_ports_are_valid(ports: &Vec<String>, name: &str) -> bool {
    // if we have a zero length vector the input is always bad
    if ports.is_empty() {
        return false;
    }

    // if the vector value of the first element is zero length the user
    // didn't specify the option and the input is valid
    if ports[0].is_empty() {
        return true;
    }

    let mut is_input_sane = true;

    for port in ports {
        match port.chars().all(char::is_numeric)
            && port.parse().unwrap_or(0) > 0
            && port.parse().unwrap_or(65536) < 65535
        {
            true => trace!("{} UDP Port is numeric. Found: {}", name, port),
            false => {
                error!(
                    "{} UDP Listen Port is not numeric or out of the range of 1-65353. Found: {}",
                    name, port
                );
                is_input_sane = false;
            }
        }
    }

    is_input_sane
}

fn check_ports_are_valid_with_host(ports: &Vec<String>, name: &str) -> bool {
    if ports.is_empty() {
        return false;
    }

    if ports[0].is_empty() {
        return true;
    }

    let mut is_input_sane = true;

    for port in ports {
        // split the host and port

        let split_port: Vec<&str> = port.split(':').collect();

        if split_port.len() != 2 {
            error!(
                "{} Port is not in the format host:port. Found: {}",
                name, port
            );
            return false;
        }

        match split_port[1].chars().all(char::is_numeric)
            && split_port[1].parse().unwrap_or(0) > 0
            && split_port[1].parse().unwrap_or(65536) < 65535
        {
            true => trace!("{} UDP Port is numeric. Found: {}", name, port),
            false => {
                error!(
                    "{} UDP Listen Port is not numeric or out of the range of 1-65353. Found: {}",
                    name, port
                );
                is_input_sane = false;
            }
        }
    }

    is_input_sane
}
