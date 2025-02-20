use std::{
    collections::{HashMap, HashSet},
    io::{BufRead, BufReader, BufWriter, Read},
    string::FromUtf8Error,
};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
};
use tracing::{error, info, warn};

const AUTHORITY_DOMAIN: &str = "pestcontrol.protohackers.com";
const AUTHORITY_PORT: u16 = 20547;

const MSG_HELLO: u8 = 0x50;
const MSG_ERROR: u8 = 0x51;
const MSG_OK: u8 = 0x52;
const MSG_DIAL_AUTHORITY: u8 = 0x53;
const MSG_TARGET_POPULATIONS: u8 = 0x54;
const MSG_CREATE_POLICY: u8 = 0x55;
const MSG_DELETE_POLICY: u8 = 0x56;
const MSG_POLICY_RESULT: u8 = 0x57;
const MSG_SITE_VISIT: u8 = 0x58;

enum Event {
    Observation { site: SiteId, counts: HashMap<String, u32> },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct SiteId(u32);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct PolicyId(u32);

async fn broadcaster(mut rx: UnboundedReceiver<Event>) -> Result<(), ReadErr> {
    let mut connections: HashMap<SiteId, (TcpStream, HashMap<String, (u32, u32)>, HashMap<String, PolicyId>)> = HashMap::new();

    while let Some(event) = rx.recv().await {
        match event {
            Event::Observation { site, counts } => {
                if !connections.contains_key(&site) {
                    let mut stream = TcpStream::connect(format!("{}:{}", AUTHORITY_DOMAIN, AUTHORITY_PORT)).await?;
                    write_msg(
                        &mut stream,
                        Msg::Hello {
                            protocol: "pestcontrol".into(),
                            version: 1,
                        },
                    )
                    .await?;
                    let msg = read_msg(&mut stream).await?;
                    if let Err(err) = validate_hello(&mut stream, msg).await {
                        error!("Error validating hello from server: {}", err);
                        continue;
                    }
                    write_msg(&mut stream, Msg::DialAuthority { site }).await?;
                    let msg = read_msg(&mut stream).await?;
                    let Msg::TargetPopulations { site, populations } = msg else {
                        error!("Unexpected response from server: {:?}", msg);
                        continue;
                    };
                    for (specie, (min, max)) in &populations {
                        info!("{}: {} - {}", specie, min, max);
                    }
                    connections.insert(site, (stream, populations, HashMap::new()));
                }
                let (stream, target_populations, policies) = connections.get_mut(&site).expect("no prepared stream");

                let mut observed_species = HashSet::new();

                for (specie, count) in counts {
                    observed_species.insert(specie.clone());

                    let Some((min, max)) = target_populations.get(&specie) else {
                        info!("{} is {} (no target)", specie, count);
                        continue;
                    };
                    info!("{} is {} (target {} - {})", specie, count, min, max);
                    if let Some(policy) = policies.get(&specie) {
                        write_msg(stream, Msg::DeletePolicy { policy: *policy }).await?;
                        policies.remove(&specie);
                        let msg = read_msg(stream).await?;
                        if !matches!(msg, Msg::Ok) {
                            error!("Misbehaving server, got msg from it: {:?}", msg);
                            continue;
                        }
                    }
                    let sent = if count < *min {
                        write_msg(
                            stream,
                            Msg::CreatePolicy {
                                specie: specie.clone(),
                                action: SpecieAction::Conserve,
                            },
                        )
                        .await?;
                        true
                    } else if count > *max {
                        write_msg(
                            stream,
                            Msg::CreatePolicy {
                                specie: specie.clone(),
                                action: SpecieAction::Cull,
                            },
                        )
                        .await?;
                        true
                    } else {
                        false
                    };
                    if sent {
                        let msg = read_msg(stream).await?;
                        if let Msg::PolicyResult { policy } = msg {
                            policies.insert(specie, policy);
                        } else {
                            error!("Misbehaving server, got msg from it: {:?}", msg);
                            continue;
                        }
                    }
                }

                for (specie, (min, _)) in target_populations {
                    if !observed_species.contains(specie) && *min > 0 {
                        if let Some(policy) = policies.get(specie) {
                            write_msg(stream, Msg::DeletePolicy { policy: *policy }).await?;
                            policies.remove(specie);
                            let msg = read_msg(stream).await?;
                            if !matches!(msg, Msg::Ok) {
                                error!("Misbehaving server, got msg from it: {:?}", msg);
                                continue;
                            }
                        }
                        write_msg(
                            stream,
                            Msg::CreatePolicy {
                                specie: specie.clone(),
                                action: SpecieAction::Conserve,
                            },
                        )
                        .await?;
                        let msg = read_msg(stream).await?;
                        if let Msg::PolicyResult { policy } = msg {
                            policies.insert(specie.clone(), policy);
                        } else {
                            error!("Misbehaving server, got msg from it: {:?}", msg);
                            continue;
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

async fn validate_hello(stream: &mut TcpStream, msg: Msg) -> Result<(), ReadErr> {
    let Msg::Hello { protocol, version } = msg else {
        write_msg(
            stream,
            Msg::Error {
                message: "where is hello?".into(),
            },
        )
        .await?;
        return Err(ReadErr::MisbehavingClient);
    };
    if protocol != "pestcontrol" || version != 1 {
        write_msg(
            stream,
            Msg::Hello {
                protocol: "pestcontrol".into(),
                version: 1,
            },
        )
        .await?;
        write_msg(
            stream,
            Msg::Error {
                message: "bad hello payload".into(),
            },
        )
        .await?;
        return Err(ReadErr::MisbehavingClient);
    }

    Ok(())
}

async fn handle_client(mut stream: TcpStream, tx: UnboundedSender<Event>) -> Result<(), ReadErr> {
    let mut errors = 0;
    loop {
        let msg = match read_msg(&mut stream).await {
            Ok(msg) => msg,
            Err(err) => {
                if let ReadErr::MessageTooLarge(size) = err {
                    warn!("got too large msg from {}: {}, quickly respond and continue reading", stream.peer_addr()?, size);
                    if errors == 0 {
                        write_msg(
                            &mut stream,
                            Msg::Hello {
                                protocol: "pestcontrol".into(),
                                version: 1,
                            },
                        )
                        .await?;
                    }
                    write_msg(
                        &mut stream,
                        Msg::Error {
                            message: format!("err reading your msg: {}", err),
                        },
                    )
                    .await?;
                    stream.flush().await.expect("flush");
                    info!("flush OK");
                    errors += 1;
                    continue;
                }

                if errors == 0 {
                    write_msg(
                        &mut stream,
                        Msg::Hello {
                            protocol: "pestcontrol".into(),
                            version: 1,
                        },
                    )
                    .await?;
                }
                write_msg(
                    &mut stream,
                    Msg::Error {
                        message: format!("err reading your msg: {}", err),
                    },
                )
                .await?;

                errors += 1;

                if let ReadErr::Io(err) = err {
                    match err.kind() {
                        std::io::ErrorKind::UnexpectedEof => {
                            stream.flush().await.expect("flush");
                            return Err(ReadErr::Io(err));
                        },
                        _ => {

                        },
                    }
                }
                continue;
            }
        };

        if validate_hello(&mut stream, msg).await.is_ok() {
            write_msg(
                &mut stream,
                Msg::Hello {
                    protocol: "pestcontrol".into(),
                    version: 1,
                },
            )
            .await?;
            break;
        } else {
            errors += 1;
        }
        if errors > 100 {
            return Err(ReadErr::TooMuchErrors)
        }
    }

    let mut errors = 0;
    loop {
        if errors > 100 {
            return Err(ReadErr::TooMuchErrors)
        }

        match read_msg(&mut stream).await {
            Ok(Msg::SiteVisit { site, populations }) => {
                let mut aggregated_populations: HashMap<String, u32> = HashMap::new();
                for (specie, count) in populations {
                    if let Some(existing_count) = aggregated_populations.get(&specie) {
                        if *existing_count != count {
                            write_msg(
                                &mut stream,
                                Msg::Error {
                                    message: "yuo sent me conflicting counts".into(),
                                },
                            )
                            .await?;
                            errors += 1;
                            continue;
                            // return Err(ReadErr::MisbehavingClient);
                        }
                    }
                    aggregated_populations.insert(specie, count);
                }
                tx.send(Event::Observation {
                    site,
                    counts: aggregated_populations,
                })
                .expect("sending to broadcaster");
            }
            Ok(_) => {
                write_msg(
                    &mut stream,
                    Msg::Error {
                        message: format!("expected to receive only SiteVisit from you"),
                    },
                )
                .await?;

                errors += 1;
            },
            Err(err) => {
                write_msg(
                    &mut stream,
                    Msg::Error {
                        message: format!("err reading your msg: {}", err),
                    },
                )
                .await?;

                if let ReadErr::Io(err) = err {
                    match err.kind() {
                        std::io::ErrorKind::UnexpectedEof => {
                            stream.flush().await.expect("flush");
                            return Err(ReadErr::Io(err));
                        },
                        _ => {

                        },
                    }
                }

                errors += 1;
            }
        }
    }
}

#[derive(Debug, Error)]
enum ReadErr {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("unknown msg type {0}")]
    UnknownMsgType(u8),
    #[error("underflow {0} != {1}")]
    Underflow(u32, usize),
    #[error("checksum fail: {0}")]
    Checksum(u8),
    #[error(transparent)]
    FromUtf8Error(#[from] FromUtf8Error),
    #[error("Misbehaving client")]
    MisbehavingClient,
    #[error("Message too large: {0}")]
    MessageTooLarge(u32),
    #[error("Too much errors")]
    TooMuchErrors,
    #[error("Too much data for little planned payload")]
    TooMuchData,
}

async fn read_msg(stream: &mut TcpStream) -> Result<Msg, ReadErr> {
    let msg_type = stream.read_u8().await?;
    let msg_len = stream.read_u32().await?;
    if msg_len > 50000 {
        return Err(ReadErr::MessageTooLarge(msg_len));
    }
    let remaining_len = msg_len - 4 - 1;

    let mut buf = vec![0; remaining_len as usize];

    let bytes_read = stream.read_exact(&mut buf).await?;
    info!("[msg type {:#04x}] bytes_read {}", msg_type, bytes_read);

    if bytes_read != remaining_len as usize {
        return Err(ReadErr::Underflow(msg_len, bytes_read));
    }

    let mut checksum: u8 = 0;
    checksum += msg_type;
    for byte in msg_len.to_be_bytes() {
        checksum += byte;
    }
    for byte in &buf {
        checksum += byte;
    }
    if checksum != 0 {
        return Err(ReadErr::Checksum(checksum));
    }

    let buf = buf.as_slice();
    let reader = BufReader::new(&buf[0..buf.len() - 1]);
    let msg = parse_msg(msg_type, reader)?;
    info!("  Got from [{}]: {}", stream.peer_addr().unwrap(), msg.dbg());
    Ok(msg)
}

fn parse_msg(msg_type: u8, mut reader: BufReader<&[u8]>) -> Result<Msg, ReadErr> {
    let msg = match msg_type {
        MSG_HELLO => {
            let protocol = read_str(&mut reader)?;
            let version = reader.read_u32::<BigEndian>()?;
            Msg::Hello { protocol, version }
        }
        MSG_ERROR => {
            let message = read_str(&mut reader)?;
            Msg::Error { message }
        }
        MSG_SITE_VISIT => {
            let site_id = SiteId(reader.read_u32::<BigEndian>()?);
            let count = reader.read_u32::<BigEndian>()?;
            let mut populations = vec![];
            info!("[dbg] going to read {}", count);
            for idx in 0..count {
                info!("[dbg] item {}", idx);
                let species = read_str_dbg(&mut reader)?;
                info!("[dbg] species {}", species);
                let count = reader.read_u32::<BigEndian>()?;
                info!("[dbg] count {}", count);
                populations.push((species, count));
            }
            Msg::SiteVisit {
                site: site_id,
                populations,
            }
        }
        MSG_OK => Msg::Ok,
        MSG_POLICY_RESULT => {
            let policy_id = PolicyId(reader.read_u32::<BigEndian>()?);
            Msg::PolicyResult { policy: policy_id }
        }
        MSG_TARGET_POPULATIONS => {
            let site_id = SiteId(reader.read_u32::<BigEndian>()?);
            let count = reader.read_u32::<BigEndian>()?;
            let mut populations = HashMap::new();
            for _ in 0..count {
                let species = read_str(&mut reader)?;
                let min = reader.read_u32::<BigEndian>()?;
                let max = reader.read_u32::<BigEndian>()?;
                populations.insert(species, (min, max));
            }
            Msg::TargetPopulations {
                site: site_id,
                populations,
            }
        }
        other => {
            error!("unknown msg type {}", other);
            return Err(ReadErr::UnknownMsgType(other));
        }
    };

    if !reader.fill_buf()?.is_empty() {
        return Err(ReadErr::TooMuchData);
    }

    Ok(msg)
}

async fn write_msg(stream: &mut TcpStream, msg: Msg) -> Result<(), std::io::Error> {
    info!("Writing to [{}]: {}", stream.peer_addr()?, msg.dbg());
    let mut buf = vec![];
    let mut writer = BufWriter::new(&mut buf);
    writer.write_u8(msg.id())?;
    writer.write_u32::<BigEndian>(0)?;
    match msg {
        Msg::Hello { protocol, version } => {
            write_str(&mut writer, &protocol)?;
            writer.write_u32::<BigEndian>(version)?;
        }
        Msg::Error { message } => {
            write_str(&mut writer, &message)?;
        }
        Msg::SiteVisit { .. } => {
            error!("Attempt to write SiteVisit");
            return Ok(());
        }
        Msg::DialAuthority { site } => {
            writer.write_u32::<BigEndian>(site.0)?;
        }
        Msg::TargetPopulations { .. } => {
            error!("Attempt to write TargetPopulations");
            return Ok(());
        }
        Msg::CreatePolicy { specie, action } => {
            write_str(&mut writer, &specie)?;
            writer.write_u8(action.id())?;
        }
        Msg::DeletePolicy { policy } => {
            writer.write_u32::<BigEndian>(policy.0)?;
        }
        Msg::PolicyResult { .. } => {
            error!("Attempt to write PolicyResult");
            return Ok(());
        }
        Msg::Ok => {
            error!("Attempt to write Ok");
            return Ok(());
        }
    }
    drop(writer);
    let msg_len = (buf.len() + 1) as u32;
    for (idx, byte) in msg_len.to_be_bytes().iter().enumerate() {
        buf[idx + 1] = *byte;
    }
    let checksum = (256 - buf.iter().sum::<u8>() as usize) as u8;
    buf.push(checksum);

    let _written = stream.write(&buf).await?;

    Ok(())
}

fn read_str(reader: &mut BufReader<&[u8]>) -> Result<String, ReadErr> {
    let str_len = reader.read_u32::<BigEndian>()?;
    let mut str_bytes = vec![0; str_len as usize];
    reader.read_exact(&mut str_bytes)?;
    Ok(String::from_utf8(str_bytes)?)
}

fn read_str_dbg(reader: &mut BufReader<&[u8]>) -> Result<String, ReadErr> {
    let str_len = reader.read_u32::<BigEndian>()?;
    info!("[dbg] str_len {}", str_len);
    let mut str_bytes = vec![0; str_len as usize];
    reader.read_exact(&mut str_bytes)?;
    Ok(String::from_utf8(str_bytes)?)
}

fn write_str(writer: &mut BufWriter<&mut Vec<u8>>, text: &str) -> Result<(), std::io::Error> {
    let len = text.len();
    writer.write_u32::<BigEndian>(len as u32)?;
    for byte in text.as_bytes() {
        writer.write_u8(*byte)?;
    }
    Ok(())
}

#[derive(Debug)]
enum Msg {
    Hello {
        protocol: String,
        version: u32,
    },
    Error {
        message: String,
    },
    SiteVisit {
        site: SiteId,
        populations: Vec<(String, u32)>,
    },
    DialAuthority {
        site: SiteId,
    },
    TargetPopulations {
        site: SiteId,
        populations: HashMap<String, (u32, u32)>,
    },
    CreatePolicy {
        specie: String,
        action: SpecieAction,
    },
    DeletePolicy {
        policy: PolicyId,
    },
    PolicyResult {
        policy: PolicyId,
    },
    Ok,
}

impl Msg {
    fn dbg(&self) -> String {
        match self {
            Msg::Hello { .. } => "Hello".into(),
            Msg::Error { message } => format!("Error: {}", message),
            Msg::SiteVisit { site, populations } => format!("Visit {}: {} recs", site.0, populations.len()),
            Msg::DialAuthority { site } => format!("Dial {}", site.0),
            Msg::TargetPopulations { site, populations } => format!("Targets {}: {} recs", site.0, populations.len()),
            Msg::CreatePolicy { specie, action } => format!("Create {}: {:?}", specie, action),
            Msg::DeletePolicy { policy } => format!("Delete {}", policy.0),
            Msg::PolicyResult { policy } => format!("Created {}", policy.0),
            Msg::Ok => format!("OK"),
        }
    }
}

#[derive(Debug)]
enum SpecieAction {
    Cull,
    Conserve,
}

impl SpecieAction {
    fn id(&self) -> u8 {
        match self {
            SpecieAction::Cull => SPECIE_ACTION_CULL,
            SpecieAction::Conserve => SPECIE_ACTION_CONSERVE,
        }
    }
}

const SPECIE_ACTION_CULL: u8 = 0x90;
const SPECIE_ACTION_CONSERVE: u8 = 0xa0;

impl Msg {
    fn id(&self) -> u8 {
        match self {
            Msg::Hello { .. } => MSG_HELLO,
            Msg::Error { .. } => MSG_ERROR,
            Msg::SiteVisit { .. } => MSG_SITE_VISIT,
            Msg::DialAuthority { .. } => MSG_DIAL_AUTHORITY,
            Msg::TargetPopulations { .. } => MSG_TARGET_POPULATIONS,
            Msg::CreatePolicy { .. } => MSG_CREATE_POLICY,
            Msg::DeletePolicy { .. } => MSG_DELETE_POLICY,
            Msg::PolicyResult { .. } => MSG_POLICY_RESULT,
            Msg::Ok { .. } => MSG_OK,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), ReadErr> {
    tracing_subscriber::fmt::init();
    let listener = TcpListener::bind("0.0.0.0:50000").await?;
    info!("Listening on port 50000");

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

    let _ = tokio::task::spawn(broadcaster(rx));

    info!("Accepting connections...");
    loop {
        let (socket, _) = listener.accept().await?;
        info!("Accepted: {}", socket.peer_addr().unwrap());
        tokio::task::spawn(handle_client(socket, tx.clone()));
    }
}
