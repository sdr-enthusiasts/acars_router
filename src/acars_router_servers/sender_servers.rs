// Copyright (c) Mike Nye, Fred Clausen
//
// Licensed under the MIT license: https://opensource.org/licenses/MIT
// Permission is granted to use, copy, modify, and redistribute the work.
// Full license information available in the project LICENSE file.
//

use crate::config_options::ACARSRouterSettings;
use crate::helper_functions::should_start_service;
use crate::udp_sender_server::UDPSenderServer;
use log::{debug, trace};
use serde_json::Value;
use tokio::net::UdpSocket;
use tokio::sync::mpsc::Receiver;

pub async fn start_sender_servers(
    config: &ACARSRouterSettings,
    mut rx_processed_acars: Receiver<Value>,
    mut rx_processed_vdlm: Receiver<Value>,
) {
    // Optional variables to store the output sockets.
    // Using optionals because the queue of messages can only have one "owner" at a time.
    // So we need to separate out the sockets from watching the queue.
    // Flow is check and see if there are any configured outputs for the queue
    // If so, start it up and save it to the appropriate server variables.
    // Then start watchers for the ACARS and VDLM queue, and once a message comes in that needs to be transmitted
    // Check if each type of server exists, and if so, send the message to it.

    let mut acars_udp_server: Option<UDPSenderServer> = None;
    let mut vdlm_udp_server: Option<UDPSenderServer> = None;

    if should_start_service(config.send_udp_acars()) {
        // Start the UDP sender servers for ACARS
        acars_udp_server =
            start_udp_senders_servers(&"ACARS".to_string(), config.send_udp_acars()).await;
    } else {
        trace!("No ACARS UDP ports to send on. Skipping");
    }

    if should_start_service(config.send_udp_vdlm2()) {
        // Start the UDP sender servers for VDLM
        vdlm_udp_server =
            start_udp_senders_servers(&"VDLM2".to_string(), config.send_udp_vdlm2()).await;
    } else {
        trace!("No VDLM2 UDP ports to send on. Skipping");
    }

    trace!("Starting the ACARS Output Queue");
    tokio::spawn(async move {
        while let Some(message) = rx_processed_acars.recv().await {
            match acars_udp_server {
                Some(ref acars_udp_server) => {
                    acars_udp_server.send_message(message).await;
                }
                None => {
                    debug!("No ACARS UDP ports to send on. Skipping");
                }
            }
        }
    });
    trace!("Starting the VDLM Output Queue");
    tokio::spawn(async move {
        while let Some(message) = rx_processed_vdlm.recv().await {
            match vdlm_udp_server {
                Some(ref vdlm_udp_server) => {
                    vdlm_udp_server.send_message(message).await;
                }
                None => {
                    debug!("No VDLM2 UDP ports to send on. Skipping");
                }
            }
        }
    });
}

async fn start_udp_senders_servers(
    decoder_type: &String,
    ports: &Vec<String>,
) -> Option<UDPSenderServer> {
    // Create an ephermeal socket for the UDP sender server
    let sock = UdpSocket::bind("0.0.0.0:0".to_string()).await.unwrap();

    let server: UDPSenderServer = UDPSenderServer {
        proto_name: decoder_type.to_string() + "_UDP_SEND",
        host: ports.clone(),
        socket: sock,
    };
    return Some(server);
}
