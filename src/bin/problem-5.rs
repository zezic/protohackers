use std::{net::{TcpListener, TcpStream, SocketAddr, Shutdown}, sync::mpsc::{Sender, self}, io::{self, BufReader, BufRead, Write}, thread};

type ClientId = usize;

fn read_line(reader: &mut BufReader<TcpStream>) -> Option<String> {
    let mut msg = String::new();
    let bytes_read = reader.read_line(&mut msg).ok()?;
    if bytes_read == 0 {
        return None;
    }
    // msg = msg.trim().to_owned();
    msg = msg.to_owned();
    if msg.len() == 0 {
        return None;
    }
    Some(msg)
}

enum Incoming {
    Tony(String),
    Chat(String),
    TonyEnd,
    ChatEnd,
}

fn tony_stream_reader(mut reader: BufReader<TcpStream>, sender: Sender<Incoming>) {
    while let Some(text) = read_line(&mut reader) {
        if let Err(err) = sender.send(Incoming::Tony(text)) {
            println!("Can't send from tony str to channel");
            return;
        }    
    }
    sender.send(Incoming::TonyEnd).unwrap();
}

fn chat_stream_reader(mut reader: BufReader<TcpStream>, sender: Sender<Incoming>) {
    while let Some(text) = read_line(&mut reader) {
        if let Err(err) = sender.send(Incoming::Chat(text)) {
            println!("Can't send from chat str to channel");
            return;
        }
    }
    sender.send(Incoming::ChatEnd).unwrap();
}

fn replace_bogus(text: String) -> String {
    let mut output = Vec::new();
    for chunk in text.split(" ") {
        if chunk.starts_with('7') && (26..36).contains(&chunk.trim().len()) && (
            chunk.chars().all(|ch| ch.is_alphanumeric()) || chunk[..chunk.len()-1].chars().all(|ch| ch.is_alphanumeric())
          ) {
            if chunk.ends_with("\n") {
                output.push("7YWHMfk9JZe0LM0g1ZauHuiSxhI\n");
            } else {
                output.push("7YWHMfk9JZe0LM0g1ZauHuiSxhI");
            }
        } else {
            output.push(chunk);
        }
    }
    // output.push("\n");
    output.join(" ")
}

fn handle_stream(
    mut tony_stream: TcpStream,
    addr: SocketAddr,
) -> Result<(), io::Error> {
    let tony_reader = BufReader::new(tony_stream.try_clone().unwrap());

    let mut chat_stream = TcpStream::connect("chat.protohackers.com:16963").unwrap();
    let chat_reader = BufReader::new(chat_stream.try_clone().unwrap());

    let (tx, rx) = mpsc::channel();

    let sender = tx.clone();
    thread::spawn(move || {
        tony_stream_reader(tony_reader, sender);
    });

    let sender = tx.clone();
    thread::spawn(move || {
        chat_stream_reader(chat_reader, sender);
    });

    let mut name = None;

    while let Ok(inc) = rx.recv() {
        match inc {
            Incoming::Tony(text) => {
                println!("[{:?}] Tony -> Serv: {}", name, text);
                let text = replace_bogus(text);
                println!("[{:?}] Serv -> Chat: {}", name, text);
                if name.is_none() {
                    name = Some(text.trim().to_string());
                }
                if let Err(err) = chat_stream.write_all(&text.as_bytes()) {
                    println!("Error writing to stream: {}", err);
                    return Ok(());
                }
                if let Err(err) = chat_stream.flush() {
                    println!("Error flushing stream: {}", err);
                }
            },
            Incoming::Chat(text) => {
                println!("[{:?}] Serv <- Chat: {}", name, text);
                let text = replace_bogus(text);
                println!("[{:?}] Tony <- Serv: {}", name, text);
                if let Err(err) = tony_stream.write_all(&text.as_bytes()) {
                    println!("Error writing to stream: {}", err);
                    return Ok(());
                }
                if let Err(err) = tony_stream.flush() {
                    println!("Error flushing stream: {}", err);
                }
            },
            Incoming::TonyEnd => {
                println!("[{:?}] TONY END {}", name, addr);
                break;
            },
            Incoming::ChatEnd => {
                println!("[{:?}] CHAT END {}", name, addr);
                break;
            },
        }
    }

    chat_stream.shutdown(Shutdown::Both).unwrap();
    println!("[{:?}] SHUTDOWN", name);

    Ok(())
}

fn main() {
    let addr = "0.0.0.0:50000";
    let server = TcpListener::bind(addr).unwrap();

    while let Ok((stream, addr)) = server.accept() {
        println!("Handling client {addr}");
        std::thread::spawn(move || handle_stream(stream, addr));
    }
}