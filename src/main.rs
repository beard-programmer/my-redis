use bytes::Bytes;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};

type Db = HashMap<String, Bytes>;
type DbHandle = Arc<Mutex<Db>>;

#[tokio::main]
async fn main() -> () {
    // Bind the listener to the address
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    let dbHandle: DbHandle = Arc::new(Mutex::new(HashMap::new()));

    loop {
        // The second item contains the IP and port of the new connection.
        let (socket, _) = listener.accept().await.unwrap();
        // A new task is spawned for each inbound socket. The socket is
        // moved to the new task and processed there.

        let dbHandle: DbHandle = dbHandle.clone();
        tokio::spawn(async move {
            process(dbHandle, socket).await;
        });
    }
}

async fn process(dbHandle: DbHandle, socket: TcpStream) {
    use mini_redis::Command;

    // Connection, provided by `mini-redis`, handles parsing frames from
    // the socket
    let mut connection = mini_redis::Connection::new(socket);

    // Use `read_frame` to receive a command from the connection.
    while let Some(frame) = connection.read_frame().await.unwrap() {
        let response = match Command::from_frame(frame).unwrap() {
            Command::Set(cmd) => {
                let mut db = dbHandle.lock().unwrap();
                db.insert(cmd.key().to_string(), cmd.value().clone());
                mini_redis::Frame::Simple("OK".to_string())
            }
            Command::Get(cmd) => {
                let db = dbHandle.lock().unwrap();
                if let Some(value) = db.get(cmd.key()) {
                    mini_redis::Frame::Bulk(value.clone())
                } else {
                    mini_redis::Frame::Null
                }
            }
            cmd => panic!("unimplemented {:?}", cmd),
        };

        // Write the response to the client
        connection.write_frame(&response).await.unwrap();
    }
}
