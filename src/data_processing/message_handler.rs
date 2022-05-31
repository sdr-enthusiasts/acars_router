use log::debug;
use tokio::sync::mpsc::Receiver;

pub async fn watch_message_queue(mut queue: Receiver<String>) {
    while let Some(message) = queue.recv().await {
        debug!("GOT: {}", message);
    }
}
