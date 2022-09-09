use std::{
    io::{self, Read, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream}, collections::BTreeMap,
};

enum MsgType {
    Insert,
    Query,
}

fn handle_stream(mut stream: TcpStream, addr: SocketAddr) -> Result<(), io::Error> {
    let mut storage = BTreeMap::new();

    loop {
        let mut header = [0; 1];
        if let Err(_err) = stream.read_exact(&mut header) {
            break;
        }
        let header = header[0] as char;
        let msg_type = match header {
            'I' => MsgType::Insert,
            'Q' => MsgType::Query,
            sym => {
                println!("{addr}: Unknown message header {sym}");
                break;
            }
        };
        let mut op_1 = [0; 4];
        if let Err(_err) = stream.read_exact(&mut op_1) {
            break;
        }
        let op_1 = i32::from_be_bytes(op_1);
        let mut op_2 = [0; 4];
        if let Err(_err) = stream.read_exact(&mut op_2) {
            break;
        }
        let op_2 = i32::from_be_bytes(op_2);
        match msg_type {
            MsgType::Insert => {
                println!("{addr}: Insert {op_1}: {op_2}");
                storage.insert(op_1, op_2);
            },
            MsgType::Query => {
                println!("{addr}: Query {op_1} -> {op_2}");
                if op_1 > op_2 {
                    let output = 0i32.to_be_bytes();
                    stream.write_all(&output)?;
                    stream.flush()?;
                    continue;
                }
                let mut count = 0;
                let mut sum = 0;
                for (_ts, item) in storage.range(op_1..=op_2) {
                    count += 1;
                    sum += *item as i128;
                }
                println!("{addr}: sum {sum}, count {count}");
                let mean = if count == 0 { 0 } else { sum / count };
                let mean_32 = mean as i32;
                let output = mean_32.to_be_bytes();
                stream.write_all(&output)?;
                stream.flush()?;
            },
        };
    }

    println!("Shutting down stream {addr}");
    stream.shutdown(Shutdown::Both)
}

fn main() {
    let addr = "0.0.0.0:50000";
    let server = TcpListener::bind(addr).unwrap();

    while let Ok((stream, addr)) = server.accept() {
        println!("Handling client {addr}");
        std::thread::spawn(move || handle_stream(stream, addr));
    }
}
