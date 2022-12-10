use bytes::Bytes;
use futures::future;
use mini_redis::client;
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
    let (tx, mut rx) = mpsc::channel(32);

    let read_futures: Vec<tokio::task::JoinHandle<()>> = (0..5)
        .into_iter()
        .map(|thread_number| {
            let transmitter = tx.clone();
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

    let write_futures: Vec<tokio::task::JoinHandle<()>> = (0..5)
        .into_iter()
        .map(|thread_number| {
            let transmitter = tx.clone();
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

    drop(tx);

    let manager = tokio::spawn(async move {
        let mut client = client::connect("127.0.0.1:6378").await.unwrap();

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
    });

    future::join_all(read_futures).await;
    future::join_all(write_futures).await;

    manager.await.unwrap();
}
