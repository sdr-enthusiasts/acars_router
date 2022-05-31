use chrono::Local;
use env_logger::Builder;
use log::{debug, info, trace};
use std::error::Error;
use std::io::Write;
use tokio::time::{sleep, Duration};

#[path = "./config_options.rs"]
mod config_options;
#[path = "./acars_router_servers/udp/udp_listener_server.rs"]
mod udp_listener_server;
use config_options::ACARSRouterSettings;
use udp_listener_server::UDPListenerServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config: ACARSRouterSettings = ACARSRouterSettings::load_values();

    let log_level = config.log_level().unwrap();
    Builder::new()
        .format(|buf, record| {
            writeln!(
                buf,
                "{} [{}] - {}",
                Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.level(),
                record.args()
            )
        })
        .filter(None, log_level)
        .init();

    config.print_values();
    // Print the log level out to the user
    info!("Log level: {:?}", config.log_level().unwrap());

    for listen_acars_udp_port in config.listen_udp_acars() {
        let listen_acars_udp_port = "127.0.0.1:".to_string() + listen_acars_udp_port.as_str();
        let server = UDPListenerServer {
            buf: vec![0; 5000],
            to_send: None,
            proto_name: "ACARS_UDP_LISTEN_".to_string() + listen_acars_udp_port.as_str(),
        };

        // // This starts the server task.
        debug!("Starting ACARS UDP server on {}", listen_acars_udp_port);
        tokio::spawn(async move { server.run(listen_acars_udp_port).await });
    }

    for listen_vdlm_udp_port in config.listen_udp_vdlm2() {
        let listen_acars_udp_port = "127.0.0.1:".to_string() + listen_vdlm_udp_port.as_str();
        let server = UDPListenerServer {
            buf: vec![0; 5000],
            to_send: None,
            proto_name: "VDLM_UDP_LISTEN_".to_string() + listen_acars_udp_port.as_str(),
        };

        // // This starts the server task.
        debug!("Starting VDLM UDP server on {}", listen_acars_udp_port);
        tokio::spawn(async move { server.run(listen_acars_udp_port).await });
    }

    // TODO: Is this the best way of doing this?
    // Without sleeping and waiting the entire program exits immediately.
    // For reasons

    trace!("Starting the sleep loop");

    loop {
        sleep(Duration::from_millis(100)).await;
    }
    Ok(())
}
