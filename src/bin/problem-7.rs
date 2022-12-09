use anyhow::{Result, anyhow};
use std::{net::{UdpSocket, SocketAddr}, str::FromStr, collections::{HashMap, BTreeMap, HashSet}, time::{Duration, Instant}, sync::mpsc::{Sender, self}, hash::Hash};

type SessionId = usize;

#[derive(Debug)]
enum Msg {
    Connect(SessionId),
    Close(SessionId),
    Data { sess_id: SessionId, pos: usize, data: String },
    Ack { sess_id: SessionId, pos: usize },
}

fn parse_sess_id(text: &str) -> Option<SessionId> {
    let session_id = text.parse::<u32>().ok()?;
    Some(session_id as usize)
}

fn parse_pos(text: &str) -> Option<usize> {
    let pos = text.parse::<u32>().ok()?;
    Some(pos as usize)
}

fn its_illegal_data(data: &String) -> bool {
    let col = data.chars().collect::<Vec<char>>();
    let wins = col.windows(3);
    for win in wins {
        if win.len() < 2 {
            continue;
        }
        if win[1] == '\\' && (win[0] != '\\' && win[2] != '\\' && win[2] != '/') {
            return true;
        }
        if win[1] == '/' && win[0] != '\\' {
            return true
        }
        if win[2] == '/' && win[1] != '\\' {
            return true
        }
    }
    false
}

fn parse_msg(text: &str) -> Option<Msg> {
    if !text.starts_with('/') {
        return None;
    }
    if !text.ends_with('/') {
        return None;
    }
    let mut parts = text.trim_matches('/').split("/");

    let msg_type = parts.next()?;

    let msg = match msg_type {
        "connect" => {
            let sess_id = parse_sess_id(parts.next()?)?;
            Msg::Connect(sess_id)
        },
        "close" => {
            let sess_id = parse_sess_id(parts.next()?)?;
            Msg::Close(sess_id)
        },
        "data" => {
            let sess_id = parse_sess_id(parts.next()?)?;
            let pos = parse_pos(parts.next()?)?;
            let mut data = parts.next()?.to_string();
            while let Some(part)= parts.next() {
                data += &format!("/{}", part);
            }
            if its_illegal_data(&data) {
                return None
            }
            Msg::Data { sess_id, pos, data }
        },
        "ack" => {
            let sess_id = parse_sess_id(parts.next()?)?;
            let pos = parse_pos(parts.next()?)?;
            Msg::Ack { sess_id, pos }
        },
        _ => { return None }
    };
    Some(msg)
}

fn escape(text: String) -> String {
    let escaped = text.replace("\\", "\\\\");
    let escaped = escaped.replace("/", "\\/");
    escaped
}

fn send_ack(socket: &UdpSocket, addr: &SocketAddr, sess_id: SessionId, length: usize) {
    let text = format!("/ack/{}/{}/", sess_id, length);
    println!("--> {} {}", addr, text);
    let buf = text.as_bytes();
    if let Err(err) = socket.send_to(buf, addr) {
        println!("Can't reply to {}: {}", addr, err);
    }
}

fn send_close(socket: &UdpSocket, addr: &SocketAddr, sess_id: SessionId) {
    let text = format!("/close/{}/", sess_id);
    println!("--> {} {}", addr, text);
    let buf = text.as_bytes();
    if let Err(err) = socket.send_to(buf, addr) {
        println!("Can't reply to {}: {}", addr, err);
    }
}

fn send_data(socket: &UdpSocket, addr: &SocketAddr, sess_id: SessionId, starts_from: usize, data: &str) {
    let data = escape(data.to_string());
    let text = format!("/data/{}/{}/{}/", sess_id, starts_from, data);
    println!("--> {} {:.40}...", addr, text.replace("\n", "<NL>"));
    let buf = text.as_bytes();
    if let Err(err) = socket.send_to(buf, addr) {
        println!("Can't reply to {}: {}", addr, err);
    }
}

fn retrans_timer(sess_id: SessionId, tx: Sender<Event>) {
    std::thread::sleep(Duration::from_secs(3));
    tx.send(Event::Retrans(sess_id)).expect("Can't send retrans")
}

fn activity_timer(tx: Sender<Event>) {
    std::thread::sleep(Duration::from_secs(3));
    tx.send(Event::ActivityCheck).expect("Can't send activ")
}

fn socket_reader(socket: UdpSocket, tx: Sender<Event>) {
    let mut buf = [0; 1500];

    while let Ok((amt, addr)) = socket.recv_from(&mut buf) {
        let msg_bytes = &buf[0..amt];
        let text = std::str::from_utf8(&msg_bytes);
        let text = match text {
            Ok(text) => {
                println!("<-- {} {:.40}...", addr, text.replace("\n", "<NL>"));
                text
            },
            Err(err) => {
                println!("Can't parse bytes {:?} as string: {}", msg_bytes, err);
                continue;
            },
        };
        let Some(msg) = parse_msg(text) else { continue; };
        tx.send(Event::Packet(msg, addr)).expect("Can't send from reader");
    }
}

enum Event {
    ActivityCheck,
    Retrans(SessionId),
    Packet(Msg, SocketAddr),
}

struct State {
    addr: SocketAddr,
    sess_id: SessionId,
    incoming: String,
    received: usize,
    outgoing: String,
    out_cursor: usize,
    tx: Sender<Event>,
    outgoing_chunks: HashMap<usize, (Instant, String)>,
    all_sent_pos_ends: Vec<usize>,
}

impl State {
    fn handle_data(&mut self, socket: &UdpSocket, pos: usize, mut data: String) {
        if pos != self.received {
            // He didn't hear that we already got!
            send_ack(&socket, &self.addr, self.sess_id, self.received);
            return;
        }

        data = data.replace("\\/", "/").replace("\\\\", "\\");

        self.received += data.len();
        send_ack(&socket, &self.addr, self.sess_id, self.received);

        // deescape
        self.incoming += &data;

        while self.incoming.contains("\n") {
            let pay = self.incoming.clone();
            let (work, rest) = pay.split_once("\n").unwrap();
            self.incoming = String::from_str(rest).unwrap();

            let work = String::from_str(work).unwrap();
            let mut rev = work.chars().rev().collect::<String>();
            rev += "\n";
            // self.outgoing += &escape(rev);
            self.outgoing += &rev;
        }

        self.maybe_resend(socket);
    }

    fn handle_ack(&mut self, socket: &UdpSocket, pos: usize) -> Result<()> {
        if !self.all_sent_pos_ends.contains(&pos) {
            return Err(anyhow!("Misbehaver!"))
        }
        self.outgoing_chunks.remove(&pos);
        self.maybe_resend(socket);
        Ok(())
    }

    fn check_old_chunks(&mut self, socket: &UdpSocket) {
        let now = Instant::now();
        for (pos, (inst, data)) in self.outgoing_chunks.iter() {
            if now.duration_since(*inst) < Duration::from_secs(1) {
                continue;
            }
            send_data(&socket, &self.addr, self.sess_id, *pos, &data);
        }
    }

    fn maybe_resend(&mut self, socket: &UdpSocket) {
        self.check_old_chunks(socket);

        let tx = self.tx.clone();
        let sess_id = self.sess_id.clone();
        std::thread::spawn(move || {
            retrans_timer(sess_id, tx);
        });

        while self.outgoing.len() > 0 {
            let mut to_send_really = String::new();
            let mut length = 0;

            let prefix = format!("/data/{}/{}//", self.sess_id, self.out_cursor);
            length += prefix.len();

            for ch in self.outgoing.chars() {
                length += match ch {
                    '\\' => 2,
                    '/' => 2,
                    _ => 1,
                };
                if length >= 800 {
                    break;
                }
                to_send_really += &format!("{}", ch);
            }

            self.outgoing = self.outgoing.split_at(to_send_really.len()).1.to_string();

            send_data(&socket, &self.addr, self.sess_id, self.out_cursor, &to_send_really);

            let len = to_send_really.len();
            self.outgoing_chunks.insert(self.out_cursor, (Instant::now(), to_send_really));
            self.out_cursor += len;
            self.all_sent_pos_ends.push(self.out_cursor);
        }
    }
}

fn main() -> std::io::Result<()> {
    let socket = UdpSocket::bind("0.0.0.0:50000")?;
    let (tx, rx) = mpsc::channel();

    let sock_clone = socket.try_clone().unwrap();

    let tx_clone = tx.clone();
    std::thread::spawn(move || {
        socket_reader(sock_clone, tx_clone)
    });

    let tx_clone = tx.clone();
    std::thread::spawn(move || {
        activity_timer(tx_clone)
    });

    let mut sessions: HashMap<usize, State> = HashMap::new();
    let mut activity: HashMap<usize, Instant> = HashMap::new();

    while let Ok(event) = rx.recv() {
        let (msg, addr) = match event {
            Event::Retrans(sess_id) => {
                if let Some(state) = sessions.get_mut(&sess_id) {
                    state.maybe_resend(&socket);
                }
                continue;
            },
            Event::Packet(msg, addr) => (msg, addr),
            Event::ActivityCheck => {
                let now = Instant::now();
                activity.retain(|sess_id, inst| {
                    if now.duration_since(*inst) > Duration::from_secs(5) {
                        if let Some(state) = sessions.remove(sess_id) {
                            println!("Close session because of inact {}: {}", sess_id, state.addr);
                            send_close(&socket, &state.addr, *sess_id);
                        }
                        false
                    } else {
                        true
                    }
                });
                continue;
            },
        };

        match msg {
            Msg::Connect(sess_id) => {
                if !sessions.contains_key(&sess_id) {
                    let tx = tx.clone();
                    sessions.insert(sess_id, State {
                        addr,
                        sess_id,
                        incoming: String::new(),
                        received: 0,
                        outgoing: String::new(),
                        out_cursor: 0,
                        tx,
                        outgoing_chunks: HashMap::new(),
                        all_sent_pos_ends: vec![0],
                    });
                }
                activity.insert(sess_id, Instant::now());
                send_ack(&socket, &addr, sess_id, 0);
            },
            Msg::Close(sess_id) => {
                println!("Close session {}: {}", sess_id, addr);
                sessions.remove(&sess_id);
                send_close(&socket, &addr, sess_id);
            },
            Msg::Data { sess_id, pos, data } => {
                match sessions.get_mut(&sess_id) {
                    Some(state) => {
                        state.handle_data(&socket, pos, data);
                    },
                    None => {
                        send_close(&socket, &addr, sess_id);
                    },
                }
                activity.insert(sess_id, Instant::now());
            },
            Msg::Ack { sess_id, pos } => {
                match sessions.get_mut(&sess_id) {
                    Some(state) => {
                        if let Err(err) = state.handle_ack(&socket, pos) {
                            println!("Err when handl ack: {}", err);
                            println!("Close session {}: {}", sess_id, addr);
                            sessions.remove(&sess_id);
                            send_close(&socket, &addr, sess_id);
                        }
                    },
                    None => {
                        send_close(&socket, &addr, sess_id);
                    }
                }
                activity.insert(sess_id, Instant::now());
            },
        };
    }

    Ok(())
}