use log::{debug, error, info, trace};
use std::error::Error;
use std::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::Sender;
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec};

pub struct TCPListenerServer {
    pub proto_name: String,
}

impl TCPListenerServer {
    pub async fn run(
        self,
        listen_acars_udp_port: String,
        channel: Sender<serde_json::Value>,
    ) -> Result<(), io::Error> {
        let TCPListenerServer { proto_name } = self;
        trace!("{}", proto_name);
        let listener = TcpListener::bind("0.0.0.0:".to_string() + &listen_acars_udp_port).await?;
        info!(
            "[TCP SERVER: {}]: Listening on: {}",
            proto_name,
            listener.local_addr()?
        );

        loop {
            trace!("Waiting for connection");
            // Asynchronously wait for an inbound TcpStream.
            let (stream, addr) = listener.accept().await?;
            let new_channel = channel.clone();
            let new_proto_name = proto_name.clone();
            debug!("accepted connection from {}", addr);
            // Spawn our handler to be run asynchronously.
            tokio::spawn(async move {
                match process_tcp_sockets(stream, new_channel).await {
                    Ok(_) => debug!("{} connection closed", new_proto_name),
                    Err(e) => error!("{} connection error: {}", new_proto_name.clone(), e),
                };
            });
        }
    }
}

async fn process_tcp_sockets(
    stream: TcpStream,
    channel: Sender<serde_json::Value>,
) -> Result<(), Box<dyn Error>> {
    let mut lines = Framed::new(stream, LinesCodec::new_with_max_length(8000));

    while let Some(Ok(line)) = lines.next().await {
        let stripped = line
            .strip_suffix("\r\n")
            .or(line.strip_suffix("\n"))
            .unwrap_or(&line);

        match serde_json::from_str::<serde_json::Value>(stripped) {
            Ok(msg) => {
                trace!("{}", msg);
                match channel.send(msg).await {
                    Ok(_) => debug!("Message sent to channel"),
                    Err(e) => error!("Error sending message to channel: {}", e),
                };
            }
            Err(e) => error!("{}", e),
        }
    }

    Ok(())
}
