use bytes::Bytes;
use futures::future;
use mini_redis::client::{self, Client};
use tokio::sync::{mpsc, oneshot};

/// Multiple different commands are multiplexed over a single channel.
#[derive(Debug)]
enum Command {
    Get {
        key: String,
        resp: Responder<Option<Bytes>>,
    },
    Set {
        key: String,
        val: Bytes,
        resp: Responder<()>,
    },
}

/// Provided by the requester and used by the manager task to send the command
/// response back to the requester.
type Responder<T> = oneshot::Sender<mini_redis::Result<T>>;

#[tokio::main]
async fn main() {
    const CHANNEL_BUFFER_SIZE: usize = 32;
    const CLIENT_ADRESS: &str = "127.0.0.1:6378";
    let (tx, rx) = mpsc::channel(CHANNEL_BUFFER_SIZE);

    let client = client::connect(CLIENT_ADRESS).await.unwrap();
    let manager = build_manager(client, rx);

    let number_of_futures = 5;
    let read_futures = spawn_get_futures(number_of_futures, tx.clone());
    let write_futures = spawn_set_futures(number_of_futures, tx);

    future::join_all(read_futures).await;
    future::join_all(write_futures).await;

    manager.await.unwrap();
}

fn build_manager(
    mut client: Client,
    mut rx: tokio::sync::mpsc::Receiver<Command>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(cmd) = rx.recv().await {
            match cmd {
                Command::Get { key, resp } => {
                    let res = client.get(&key).await;
                    // Ignore errors
                    let _ = resp.send(res);
                }
                Command::Set { key, val, resp } => {
                    let res = client.set(&key, val).await;
                    // Ignore errors
                    let _ = resp.send(res);
                }
            }
        }
    })
}

fn spawn_get_futures(
    number_of_futures: usize,
    transmitter: tokio::sync::mpsc::Sender<Command>,
) -> Vec<tokio::task::JoinHandle<()>> {
    let read_futures: Vec<tokio::task::JoinHandle<()>> = (0..number_of_futures)
        .into_iter()
        .map(|thread_number| {
            let transmitter = transmitter.clone();
            tokio::spawn(async move {
                let (resp_tx, resp_rx) = oneshot::channel();
                let key = format!("foo#{}", thread_number).into();
                let cmd = Command::Get { key, resp: resp_tx };

                if transmitter.send(cmd).await.is_err() {
                    eprintln!("connection task shutdown");
                    return;
                }

                // Await the response
                let res = resp_rx.await;
                println!("GOT (Get) = {:?}", res);
            })
        })
        .collect();
    read_futures
}

fn spawn_set_futures(
    number_of_futures: usize,
    transmitter: tokio::sync::mpsc::Sender<Command>,
) -> Vec<tokio::task::JoinHandle<()>> {
    let write_futures: Vec<tokio::task::JoinHandle<()>> = (0..number_of_futures)
        .into_iter()
        .map(|thread_number| {
            let transmitter = transmitter.clone();
            tokio::spawn(async move {
                let (resp_tx, resp_rx) = oneshot::channel();
                let key = format!("foo#{}", thread_number).into();
                let val = format!("bar#{}", thread_number).into();
                let cmd = Command::Set {
                    key,
                    val,
                    resp: resp_tx,
                };

                if transmitter.send(cmd).await.is_err() {
                    eprintln!("connection task shutdown");
                    return;
                }

                let res = resp_rx.await;
                println!("GOT (Set) = {:?}", res);
            })
        })
        .collect();
    write_futures
}
