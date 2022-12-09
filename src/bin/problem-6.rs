use core::time;
use std::{
    collections::{HashMap, VecDeque, HashSet},
    io::{self, ErrorKind, Read, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream},
    sync::mpsc::{self, Receiver, Sender, RecvTimeoutError},
    time::Duration, hash::Hash,
};

use uuid::Uuid;

enum ClientType {
    Camera,
    Dispatcher,
}

enum Welcome {
    Client(ClientType),
    Hearter(Receiver<HBMarker>),
}

struct HBMarker;

impl MakeHB<HBMarker> for HBMarker {
    fn make_hb() -> HBMarker {
        HBMarker
    }
}

fn read_cl_type(stream: &mut TcpStream) -> Result<Welcome, io::Error> {
    let mut type_id = [0];
    stream.read_exact(&mut type_id)?;

    let client_type = match type_id[0] {
        0x80 => Welcome::Client(ClientType::Camera),
        0x81 => Welcome::Client(ClientType::Dispatcher),
        0x40 => {
            match read_u32(stream) {
                Ok(interval) => {
                    let (tx, rx) = mpsc::channel();
                    std::thread::spawn(move || {
                        let int = match interval {
                            0 => Duration::from_secs(1000 * 1000),
                            x => Duration::from_millis((x * 100) as u64),
                        };
                        heartbeater::<HBMarker>(int, tx)
                    });
                    return Ok(Welcome::Hearter(rx));
                }
                Err(err) => {
                    return Err(io::Error::from(ErrorKind::InvalidData));
                }
            }
        }, 
        x => {
            reject(stream);
            return Err(io::Error::from(ErrorKind::InvalidData));
        }
    };

    Ok(client_type)
}

fn send_error(stream: &mut TcpStream) {
    let mut msg = vec![];
    msg.push(0x10);

    let text = "it's very baaaad";
    let text_bytes = text.as_bytes();

    msg.push(text_bytes.len() as u8);
    msg.extend_from_slice(text_bytes);

    if let Err(err) = stream.write_all(&msg) {
    }
}

enum Finally {
    Client(ClientType),
    Misbehaver,
}

fn cl_type_reader(
    mut stream: TcpStream,
    cltype_tx: Sender<Finally>
) {
    let type_id = read_u8(&mut stream);
    if let Err(err) = type_id {
        reject(&mut stream);
        cltype_tx.send(Finally::Misbehaver).expect("Can't tell about misbehaver");
        return;
    }

    let client_type = match type_id.unwrap() {
        0x80 => Finally::Client(ClientType::Camera),
        0x81 => Finally::Client(ClientType::Dispatcher),
        x => {
            reject(&mut stream);
            cltype_tx.send(Finally::Misbehaver);
            return;
        }
    };

    cltype_tx.send(client_type);
}

fn handle_stream(
    mut stream: TcpStream,
    addr: SocketAddr,
    etx: Sender<EngineMsg>,
) -> Result<(), io::Error> {
    let cl_type = read_cl_type(&mut stream);

    if let Err(err) = cl_type {
        return Ok(());
    }
    let cl_type = cl_type.unwrap();

    match cl_type {
        Welcome::Client(client_type) => {
            match client_type {
                ClientType::Camera => handle_camera(stream, etx, None),
                ClientType::Dispatcher => handle_dispatcher(stream, etx, None),
            };
        },
        Welcome::Hearter(hrx) => {
            let stream_clone = stream.try_clone().expect("Can't clone stream");
            let (cltype_tx, cltype_rx) = mpsc::channel();
            std::thread::spawn(move || cl_type_reader(stream_clone, cltype_tx));

            loop {
                if let Ok(finally) = cltype_rx.try_recv() {
                    match finally {
                        Finally::Client(client_type) => {
                            match client_type {
                                ClientType::Camera => handle_camera(stream, etx, Some(hrx)),
                                ClientType::Dispatcher => handle_dispatcher(stream, etx, Some(hrx)),
                            };
                            return Ok(());
                        },
                        Finally::Misbehaver => {
                            reject(&mut stream);
                            return Ok(());
                        },
                    }
                }
                std::thread::sleep(Duration::from_millis(50));
                if let Ok(h) = hrx.try_recv() {
                    if let Err(err) = send_hb(&mut stream) {
                        reject(&mut stream);
                        return Ok(());
                    }
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        },
    }

    Ok(())
}

fn read_u8(stream: &mut TcpStream) -> Result<u8, io::Error> {
    let mut buf = [0; 1];
    stream.read_exact(&mut buf)?;
    Ok(u8::from_be_bytes(buf))
}

fn read_u16(stream: &mut TcpStream) -> Result<u16, io::Error> {
    let mut buf = [0; 2];
    stream.read_exact(&mut buf)?;
    Ok(u16::from_be_bytes(buf))
}

fn read_u32(stream: &mut TcpStream) -> Result<u32, io::Error> {
    let mut buf = [0; 4];
    stream.read_exact(&mut buf)?;
    Ok(u32::from_be_bytes(buf))
}

fn handle_camera(mut stream: TcpStream, tx: Sender<EngineMsg>, hrx: Option<Receiver<HBMarker>>) {
    let mut setup = || {
        let road = read_u16(&mut stream)?;
        let mile = read_u16(&mut stream)?;
        let limit = read_u16(&mut stream)?;
        Ok((road, mile, limit))
    };
    let setup: Result<(u16, u16, u16), io::Error> = setup();
    match setup {
        Ok(setup) => {
            let tx = tx.clone();
            std::thread::spawn(move || camera_mainloop(stream, tx, setup, hrx));
        }
        Err(err) => {
            reject(&mut stream);
        }
    }
}

fn handle_dispatcher(mut stream: TcpStream, tx: Sender<EngineMsg>, hrx: Option<Receiver<HBMarker>>) {
    let mut setup = || {
        let numroads = read_u8(&mut stream)? as usize;
        let mut roads = vec![];
        for _ in 0..numroads {
            let road = read_u16(&mut stream)?;
            roads.push(road)
        }
        Ok(roads)
    };
    let setup: Result<Vec<u16>, io::Error> = setup();
    match setup {
        Ok(setup) => {
            let tx = tx.clone();
            std::thread::spawn(move || dispatcher_mainloop(stream, tx, setup, hrx));
        }
        Err(err) => {
            reject(&mut stream);
        }
    }
}

enum CameraFeed {
    Heartbeat,
    Packet(CameraPkt),
    Gone,
}

enum DispatcherFeed {
    Heartbeat,
    Packet(DispatcherPkt),
    Fine(Fine),
    Gone,
}

enum CameraPkt {
    WantHeartbeat(u32),
    Plate(String, u32),
}

enum DispatcherPkt {
    WantHeartbeat(u32),
}

trait MakeHB<T> {
    fn make_hb() -> T;
}

impl MakeHB<CameraFeed> for CameraFeed {
    fn make_hb() -> CameraFeed {
        CameraFeed::Heartbeat
    }
}

impl MakeHB<DispatcherFeed> for DispatcherFeed {
    fn make_hb() -> DispatcherFeed {
        DispatcherFeed::Heartbeat
    }
}

fn heartbeater<T>(interval: Duration, tx: Sender<T>)
where
    T: MakeHB<T>,
{
    loop {
        std::thread::sleep(interval);
        if let Err(err) = tx.send(T::make_hb()) {
            break;
        }
    }
}

fn read_string(stream: &mut TcpStream) -> Result<String, io::Error> {
    let len = read_u8(stream)? as usize;
    let mut text = vec![0; len];
    text.resize(len, 0u8);
    stream.read_exact(&mut text)?;
    String::from_utf8(text).map_err(|_| io::Error::from(io::ErrorKind::InvalidData))
}

fn camera_reader(mut stream: TcpStream, tx: Sender<CameraFeed>) {
    loop {
        let mut pkt_id = [0];
        if let Err(_err) = stream.read_exact(&mut pkt_id) {
            tx.send(CameraFeed::Gone).expect("Can't send CameraFeed");
            return;
        }

        match pkt_id[0] {
            0x20 => {
                let plate = match read_string(&mut stream) {
                    Ok(text) => text,
                    Err(err) => {
                        reject(&mut stream);
                        break;
                    }
                };
                let timestamp = match read_u32(&mut stream) {
                    Ok(ts) => ts,
                    Err(err) => {
                        reject(&mut stream);
                        break;
                    }
                };
                tx.send(CameraFeed::Packet(CameraPkt::Plate(plate, timestamp)))
                    .expect("Can't send to camera mainloop");
            }
            0x40 => match read_u32(&mut stream) {
                Ok(interval) => {
                    tx.send(CameraFeed::Packet(CameraPkt::WantHeartbeat(interval)))
                        .expect("Can't send to camera mainloop");
                }
                Err(err) => {
                    reject(&mut stream);
                    break;
                }
            },
            x => {
                reject(&mut stream);
                break;
            }
        };
    }
}

fn dispatcher_reader(mut stream: TcpStream, tx: Sender<DispatcherFeed>) {
    loop {
        let mut pkt_id = [0];
        if let Err(_err) = stream.read_exact(&mut pkt_id) {
            tx.send(DispatcherFeed::Gone).expect("Can't send DispatcherFeed");
            return;
        }

        match pkt_id[0] {
            0x40 => match read_u32(&mut stream) {
                Ok(interval) => {
                    tx.send(DispatcherFeed::Packet(DispatcherPkt::WantHeartbeat(interval)))
                        .expect("Can't send to dispatcher mainloop");
                }
                Err(err) => {
                    reject(&mut stream);
                    break;
                }
            },
            x => {
                reject(&mut stream);
                break;
            }
        };
    }
}

fn send_hb(stream: &mut TcpStream) -> Result<(), io::Error> {
    let msg = [0x41];
    stream.write_all(&msg)?;
    Ok(())
}

fn reject(mut stream: &mut TcpStream) {
    send_error(&mut stream);
    stream
        .shutdown(Shutdown::Both).ok();
}

fn dispatcher_mainloop(mut stream: TcpStream, etx: Sender<EngineMsg>, roads: Vec<u16>, hrx: Option<Receiver<HBMarker>>) {
    let uuid = Uuid::new_v4();

    let (dtx, drx) = mpsc::channel();

    let mut requested_hb = match hrx {
        Some(hrx) => {
            let dtx = dtx.clone();
            std::thread::spawn(move || hb_retrans(hrx, dtx));
            true
        },
        None => false,
    };

    let dtx_clone = dtx.clone();
    etx.send(EngineMsg::NewDispatcher { uuid, roads, sender: dtx_clone })
        .expect("Can't send to engine");

    let stream_clone = stream.try_clone().expect("Can't clone stream");
    let rdr_dtx = dtx.clone();
    std::thread::spawn(|| dispatcher_reader(stream_clone, rdr_dtx));

    while let Ok(feed) = drx.recv() {
        match feed {
            DispatcherFeed::Packet(pkt) => {
                match pkt {
                    DispatcherPkt::WantHeartbeat(interval) => {
                        if requested_hb {
                            reject(&mut stream);
                            break;
                        }
                        requested_hb = true;
                        if interval == 0 {
                            continue;
                        }
                        let dtx = dtx.clone();
                        std::thread::spawn(move || {
                            let int = Duration::from_millis((interval * 100) as u64);
                            heartbeater::<DispatcherFeed>(int, dtx)
                        });
                    }
                }
            },
            DispatcherFeed::Gone => {
                break;
            },
            DispatcherFeed::Heartbeat => {
                if let Err(err) = send_hb(&mut stream) {
                    break;
                }
            }
            DispatcherFeed::Fine(fine) => {
                if let Err(err) = fine.send(&mut stream) {
                    reject(&mut stream);
                    break;
                }
            },
        }
    }
}

fn hb_retrans<T>(hrx: Receiver<HBMarker>, htx: Sender<T>) where T: MakeHB<T> {
    while let Ok(h) = hrx.recv() {
        htx.send(T::make_hb()).expect("Can't retrans HB");
    }
}

fn camera_mainloop(mut stream: TcpStream, etx: Sender<EngineMsg>, setup: (u16, u16, u16), hrx: Option<Receiver<HBMarker>>) {
    let (road, mile, limit) = setup;
    let uuid = Uuid::new_v4();

    let (ctx, crx) = mpsc::channel();

    let mut requested_hb = match hrx {
        Some(hrx) => {
            let ctx = ctx.clone();
            std::thread::spawn(move || hb_retrans(hrx, ctx));
            true
        },
        None => false,
    };

    etx.send(EngineMsg::NewCamera {
        uuid,
        road,
        mile,
        limit,
    })
    .expect("Can't send to engine");

    let stream_clone = stream.try_clone().expect("Can't clone stream");
    let rdr_ctx = ctx.clone();
    std::thread::spawn(|| camera_reader(stream_clone, rdr_ctx));

    while let Ok(feed) = crx.recv() {
        match feed {
            CameraFeed::Heartbeat => {
                if let Err(err) = send_hb(&mut stream) {
                    break;
                }
            }
            CameraFeed::Packet(pkt) => match pkt {
                CameraPkt::WantHeartbeat(interval) => {
                    if requested_hb {
                        reject(&mut stream);
                        break;
                    }
                    requested_hb = true;
                    if interval == 0 {
                        continue;
                    }
                    let ctx = ctx.clone();
                    std::thread::spawn(move || {
                        let int = Duration::from_millis((interval * 100) as u64);
                        heartbeater::<CameraFeed>(int, ctx)
                    });
                }
                CameraPkt::Plate(plate, timestamp) => {
                    etx.send(EngineMsg::NewPlate {
                        uuid,
                        plate,
                        road,
                        mile,
                        timestamp,
                    })
                    .expect("Can't send to engine");
                }
            },
            CameraFeed::Gone => {
            },
        }
    }
}

enum EngineMsg {
    NewCamera {
        uuid: Uuid,
        road: u16,
        mile: u16,
        limit: u16,
    },
    NewPlate {
        uuid: Uuid,
        plate: String,
        road: u16,
        mile: u16,
        timestamp: u32,
    },
    NewDispatcher {
        uuid: Uuid,
        roads: Vec<u16>,
        sender: Sender<DispatcherFeed>,
    },
}

struct Dispatcher {
    roads: Vec<u16>,
    sender: Sender<DispatcherFeed>,
}

#[derive(Clone)]
struct Fine {
    plate: String,
    road: u16,
    mile1: u16,
    timestamp1: u32,
    mile2: u16,
    timestamp2: u32,
    speed: u16, // (100x miles per hour)
}

impl Fine {
    fn send(self, stream: &mut TcpStream) -> Result<(), io::Error> {
        // Header
        stream.write_all(&[0x21])?;

        let Fine { plate, road, mile1, timestamp1, mile2, timestamp2, speed } = self;

        let plate_len = plate.len() as u8;
        stream.write_all(&[plate_len])?;
        let plate_txt = plate.as_bytes();
        stream.write_all(&plate_txt)?;

        let road = road.to_be_bytes();
        stream.write_all(&road)?;

        let mile1 = mile1.to_be_bytes();
        stream.write_all(&mile1)?;

        let timestamp1 = timestamp1.to_be_bytes();
        stream.write_all(&timestamp1)?;

        let mile2 = mile2.to_be_bytes();
        stream.write_all(&mile2)?;

        let timestamp2 = timestamp2.to_be_bytes();
        stream.write_all(&timestamp2)?;

        let speed = speed.to_be_bytes();
        stream.write_all(&speed)?;

        Ok(())
    }
}

fn engine(rx: Receiver<EngineMsg>) {
    let mut dispatchers: HashMap<Uuid, Dispatcher> = HashMap::new();
    let mut road_dispatchers: HashMap<u16, HashSet<Uuid>> = HashMap::new();

    let mut road_limits: HashMap<u16, u16> = HashMap::new();

    let mut road_plate_timestamps: HashMap<(u16, String), Vec<(u16, u32)>> = HashMap::new();

    let mut plate_fine_days = HashMap::new();

    let mut fine_queue = VecDeque::new();

    while let Ok(msg) = rx.recv() {
        match msg {
            EngineMsg::NewCamera {
                uuid,
                road,
                mile,
                limit,
            } => {
                road_limits.insert(road, limit);
            }
            EngineMsg::NewPlate {
                uuid,
                plate,
                timestamp,
                road,
                mile,
            } => {
                let combo_key = (road, plate.clone());
                let records = road_plate_timestamps.entry(combo_key.clone()).or_insert(vec![]);

                let days = plate_fine_days.entry(plate.clone()).or_insert(vec![]);
                let limit = *road_limits.get(&road).expect("Can't find road limit");

                let cur_day = (timestamp as f64 / 86400.0).floor() as u32;

                let mut ignored = true;

                for (old_mile, old_timestamp) in records.iter() {
                    let old_day = (*old_timestamp as f64 / 86400.0).floor() as u32;

                    if days.contains(&old_day) || days.contains(&cur_day) {
                        continue;
                    }

                    let time = old_timestamp.abs_diff(timestamp);
                    let dist = old_mile.abs_diff(mile);
                    if dist == 0 {
                        continue;
                    }
                    let speed = ((dist as f64 / (time as f64 / 60.0 / 60.0))).round();
                    let overkek = speed - limit as f64;
                    if overkek < 0.5 {
                        continue;
                    }

                    let (start_day, end_day) = if timestamp > *old_timestamp { (old_day, cur_day) } else { (cur_day, old_day) };
                    let (timestamp1, timestamp2) = if timestamp > *old_timestamp { (*old_timestamp, timestamp) } else { (timestamp, *old_timestamp) };

                    for day in start_day..=end_day {
                        days.push(day);
                    }
                    ignored = false;
                    let (mile1, mile2) = if timestamp > *old_timestamp { (*old_mile, mile) } else { (mile, *old_mile) };
                    println!("[TICKET] {plate} at days {start_day}->{end_day} ({timestamp1}->{timestamp2})");
                    let speed = (speed * 100.0).floor() as u16;
                    let fine = Fine {
                        plate: plate.clone(),
                        road,
                        mile1,
                        timestamp1,
                        mile2,
                        timestamp2,
                        speed,
                    };
                    if let Some(disps) = road_dispatchers.get(&road) {
                        let disp_id = disps.iter().next().expect("Can't take firt disp_id");
                        let disp = dispatchers.get_mut(disp_id).expect("No dispatcher!");
                        disp.sender.send(DispatcherFeed::Fine(fine)).expect("Can't send to dispatcher");
                    } else {
                        fine_queue.push_back(fine);
                    }
                    break;
                }

                if ignored {
                    println!("[IGNORE] {plate} at {cur_day}");
                }

                records.push((mile, timestamp));
            },
            EngineMsg::NewDispatcher { uuid, roads , sender } => {
                for road in &roads {
                    let disps = road_dispatchers.entry(*road).or_insert(HashSet::new());
                    disps.insert(uuid);
                }
                fine_queue.retain(|fine| {
                    if roads.contains(&fine.road) {
                        if let Err(err) = sender.send(DispatcherFeed::Fine(fine.clone())) {
                        }
                        false
                    } else {
                        true
                    }
                });
                dispatchers.insert(uuid, Dispatcher { roads, sender });
            },
        }
    }
}

fn main() {
    let addr = "0.0.0.0:50000";
    let server = TcpListener::bind(addr).unwrap();

    let (etx, erx) = mpsc::channel();

    std::thread::spawn(move || engine(erx));

    while let Ok((stream, addr)) = server.accept() {
        let stream_etx = etx.clone();
        std::thread::spawn(move || handle_stream(stream, addr, stream_etx));
    }
}
