use std::{
    collections::HashMap,
    io::{self, BufRead, BufReader, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream},
    sync::mpsc::{self, Receiver, Sender},
};

type ClientId = usize;

enum ClientEvent {
    NewClient(ClientId, TcpStream, String),
    Message(ClientId, String),
    Disconnect(ClientId),
}

fn read_line(reader: &mut BufReader<TcpStream>) -> Option<String> {
    let mut msg = String::new();
    let bytes_read = reader.read_line(&mut msg).ok()?;
    if bytes_read == 0 {
        return None;
    }
    msg = msg.trim().to_owned();
    if msg.len() == 0 {
        return None;
    }
    Some(msg)
}

fn handle_stream(
    mut stream: TcpStream,
    addr: SocketAddr,
    tx: Sender<ClientEvent>,
    client_id: ClientId,
) -> Result<(), io::Error> {
    println!("{addr} connected as client {client_id}");
    stream.write_all(&"Welcome to budgetchat! What shall I call you?\n".as_bytes())?;
    stream.flush()?;

    let mut reader = BufReader::new(stream.try_clone().unwrap());

    let nickname = read_line(&mut reader).ok_or(io::Error::from(io::ErrorKind::InvalidData))?;

    if !nickname.chars().all(|chr| chr.is_ascii_alphanumeric()) {
        println!("{addr} has bad nick: {}", nickname);
        stream
            .shutdown(Shutdown::Both)
            .expect("Can't shutdown stream");
        return Ok(());
    }

    println!("{addr} setting nickname to: {nickname}");
    tx.send(ClientEvent::NewClient(
        client_id,
        stream.try_clone().expect("Can't clone stream"),
        nickname,
    ))
    .expect("Can't send to channel");

    loop {
        let msg = match read_line(&mut reader) {
            Some(text) => text,
            None => break,
        };
        println!("{addr}: {msg}");
        tx.send(ClientEvent::Message(client_id, msg))
            .expect("Can't send to channel");
    }

    tx.send(ClientEvent::Disconnect(client_id))
        .expect("Can't send to channel");

    stream.shutdown(Shutdown::Both)
}

fn broadcaster(rx: Receiver<ClientEvent>) {
    let mut clients: HashMap<usize, (TcpStream, String)> = HashMap::new();

    while let Ok(request) = rx.recv() {
        match request {
            ClientEvent::NewClient(client_id, mut stream, nickname) => {
                let text = format!("* {} has entered the room\n", nickname);

                clients
                    .iter_mut()
                    .map(|(_id, (stream, _nick))| stream)
                    .for_each(|stream| {
                        if let Err(err) = stream.write_all(&text.as_bytes()) {
                            println!("Error writing to stream: {}", err);
                            return;
                        }
                        if let Err(err) = stream.flush() {
                            println!("Error flushing stream: {}", err);
                        }
                    });

                let all_nicks: String = clients
                    .values()
                    .map(|(_, nick)| nick.clone())
                    .collect::<Vec<String>>()
                    .join(", ");

                let text = format!("* The room contains: {}\n", all_nicks);

                if let Err(err) = stream.write_all(&text.as_bytes()) {
                    println!("Error writing to stream: {}", err);
                    continue;
                }

                clients.insert(client_id, (stream, nickname));
            }
            ClientEvent::Message(client_id, msg) => {
                let (_stream, nickname) = &clients.get(&client_id).expect("Can't find client");
                let text = format!("[{}] {}\n", nickname, msg);

                clients
                    .iter_mut()
                    .filter(|(&stored_client_id, _)| stored_client_id != client_id)
                    .map(|(_id, (stream, _nick))| stream)
                    .for_each(|stream| {
                        if let Err(err) = stream.write_all(&text.as_bytes()) {
                            println!("Error writing to stream: {}", err);
                            return;
                        }
                        if let Err(err) = stream.flush() {
                            println!("Error flushing stream: {}", err);
                        }
                    })
            }
            ClientEvent::Disconnect(client_id) => {
                let client = clients.remove(&client_id);
                if client.is_none() {
                    continue;
                }
                let nickname = client
                    .expect("Trying to remove non-existent client")
                    .1
                    .clone();

                let text = format!("* {} has left the room\n", nickname);

                clients
                    .iter_mut()
                    .filter(|(&stored_client_id, _)| stored_client_id != client_id)
                    .map(|(_id, (stream, _nick))| stream)
                    .for_each(|stream| {
                        if let Err(err) = stream.write_all(&text.as_bytes()) {
                            println!("Error writing to stream: {}", err);
                            return;
                        }
                        if let Err(err) = stream.flush() {
                            println!("Error flushing stream: {}", err);
                        }
                    })
            }
        }
    }
}

fn main() {
    let addr = "0.0.0.0:50000";
    let server = TcpListener::bind(addr).unwrap();

    let mut last_client_id = 0;

    let (tx, rx) = mpsc::channel();

    std::thread::spawn(move || broadcaster(rx));

    while let Ok((stream, addr)) = server.accept() {
        println!("Handling client {addr}");
        last_client_id += 1;
        let client_id = last_client_id;
        let client_tx = tx.clone();
        std::thread::spawn(move || handle_stream(stream, addr, client_tx, client_id));
    }
}
