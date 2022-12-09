use std::{
    io::{self, Read, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream, UdpSocket}, collections::HashMap,
};

fn main() {
    let addr = "0.0.0.0:50000";
    let socket = UdpSocket::bind(addr).unwrap();

    let mut buf = vec![0; 2000];

    let mut db = HashMap::new();
    db.insert("version".to_string(), "zezic's Key-Value Store 1.0".to_string());

    while let Ok((amt, addr)) = socket.recv_from(&mut buf) {
        println!("Handling client {addr}, read {amt} bytes");
        let req = std::str::from_utf8(&buf[..amt]).unwrap();
        println!("req: {}", req);

        if let Some((key, value)) = req.split_once('=') {
            if key == "version" {
                continue;
            }
            db.insert(key.to_string(), value.to_string());
        } else {
            let empty = String::new();
            let rec = db.get(req).unwrap_or(&empty);
            let response = format!("{req}={rec}");
            if let Err(err) = socket.send_to(&response.as_bytes(), &addr) {
                println!("Error sending to client {addr}: {}", err);
            }
        }
    }
}
