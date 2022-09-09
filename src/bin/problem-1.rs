use std::{
    io::{self, Read, Write, BufReader, BufRead},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream},
};

use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Request {
    method: String,
    number: f64,
}

#[derive(Serialize)]
struct Response {
    method: String,
    prime: bool,
}

fn parse_req(buf: &str) -> Result<Request, io::Error> {
    let buf = match buf.split('\n').next() {
        Some(slc) => slc,
        None => return Err(io::Error::from(io::ErrorKind::InvalidData))
    };
    let req: Request = serde_json::from_str(&buf.trim())?;
    if req.method != "isPrime" {
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    }
    Ok(req)
}

fn check_prime(num: f64) -> bool {
    if num % 1.0 != 0.0 || num < 0.0 {
        return false
    }
    let num = num as u64;
    primes::is_prime(num)
}

fn handle_stream(mut stream: TcpStream, addr: SocketAddr) -> Result<(), io::Error> {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    loop {
        let mut json = String::new();
        let bytes_read = reader.read_line(&mut json).unwrap();
        if bytes_read == 0 {
            break;
        }
        if json.trim().len() == 0 {
            continue;
        }
        println!("{addr}: {json}");
        let req = match parse_req(&json) {
            Ok(req) => req,
            Err(err) => {
                println!("{addr}: Malformed request: {err}");
                stream.write_all(&[1, 2, 3])?;
                break;
            }
        };

        let is_prime = check_prime(req.number);
        println!("{addr}: {} is_prime: {is_prime}", req.number);

        let resp = Response {
            method: "isPrime".into(),
            prime: is_prime
        };

        let mut out_buf = serde_json::to_string(&resp)?;
        out_buf += "\n";

        stream.write_all(&out_buf.as_bytes())?;
        stream.flush()?;
    }

    // println!("Shutting down stream {addr}");
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
