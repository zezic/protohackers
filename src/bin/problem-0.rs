use std::{
    io::{self, Read, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream},
};

fn handle_stream(mut stream: TcpStream, addr: SocketAddr) -> Result<(), io::Error> {
    let mut buf = vec![];

    let bytes_read = stream.read_to_end(&mut buf)?;
    println!("Read {bytes_read} bytes from {addr}");

    stream.write_all(&buf)?;

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
