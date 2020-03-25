use std::{
    io::{self, BufRead, BufReader, Error, ErrorKind, Read, Write},
    net::TcpStream,
    str::FromStr,
    sync::Arc,
};

use nom::{
    bytes::streaming::{take_until, take_while, take_while1},
    character::is_space,
    character::streaming::crlf,
    sequence::tuple,
    Err::Incomplete,
    IResult,
};

use crate::{Message, ServerInfo, SharedState};

// Protocol
const INFO: &[u8] = b"INFO";
const MSG: &[u8] = b"MSG";
const PING: &[u8] = b"PING";
const PONG: &[u8] = b"PONG";
const ERR: &[u8] = b"-ERR";

#[inline]
fn is_valid_op_char(c: u8) -> bool {
    (c >= 0x41 && c <= 0x5A) || c == b'-' || c == b'+'
}

pub(crate) struct ReadLoopState {
    pub(crate) reader: BufReader<TcpStream>,
    pub(crate) shared_state: Arc<SharedState>,
}

pub(crate) fn read_loop(state: &mut ReadLoopState) -> io::Result<()> {
    loop {
        let parsed_op = parse_control_op(&mut state.reader)?;
        match parsed_op {
            ControlOp::Msg(msg_args) => state.process_msg(msg_args)?,
            ControlOp::Ping => state.send_pong()?,
            ControlOp::Pong => state.process_pong(),
            ControlOp::EOF => return Ok(()),
            ControlOp::Info(_) | ControlOp::Err(_) | ControlOp::Unknown(_) => {
                eprintln!("Received unhandled message: {:?}", parsed_op)
            }
        }
    }
}

impl ReadLoopState {
    fn reconnect(&mut self) -> io::Result<()> {
        // flush outstanding pongs
        {
            let mut pongs = self.shared_state.pongs.lock().unwrap();
            while let Some(s) = pongs.pop_front() {
                s.send(true).unwrap();
            }
        }

        todo!("clear any captured errors");
        /*
        todo!("execute disconnect callback if registered");

        // for each server in the server pool:
        todo!("wait backoff");
        todo!("record retry stats");
        todo!("clear particular server stats");
        todo!("resend subscriptions");
        todo!("send the buffered items");
        todo!("trigger reconnected callback");
        todo!("flush buffers to server");
        Ok(())
        */
    }
    fn process_pong(&mut self) {
        let mut pongs = self.shared_state.pongs.lock().unwrap();
        if let Some(s) = pongs.pop_front() {
            s.send(true).unwrap();
        }
    }

    fn send_pong(&self) -> io::Result<()> {
        let w = &mut self.shared_state.writer.lock().unwrap().writer;
        w.write_all(b"PONG\r\n")?;
        w.flush()?;
        Ok(())
    }

    fn process_msg(&mut self, msg_args: MsgArgs) -> io::Result<()> {
        let mut msg = Message {
            subject: msg_args.subject,
            reply: msg_args.reply,
            data: Vec::with_capacity(msg_args.mlen as usize),
            responder: None,
        };

        // Setup so we can send responses.
        if msg.reply.is_some() {
            msg.responder = Some(self.shared_state.clone());
        }

        let reader = &mut self.reader;
        // FIXME(dlc) - avoid copy if possible.
        // FIXME(dlc) - Just read CRLF? Buffered so should be ok.
        reader
            .take(u64::from(msg_args.mlen))
            .read_to_end(&mut msg.data)?;

        let mut crlf = [0; 2];
        reader.read_exact(&mut crlf)?;

        // Now lookup the subscription's channel.
        let subs = self.shared_state.subs.read().unwrap();
        if let Some(tx) = subs.get(&msg_args.sid) {
            tx.send(msg).unwrap();
        }
        Ok(())
    }
}

pub(crate) fn parse_control_op(reader: &mut BufReader<TcpStream>) -> io::Result<ControlOp> {
    // This should not do a malloc here so this should be ok.
    let mut buf = Vec::new();
    let (input, start_len, (op, args)) = {
        if let Some((input, start_len, (op, args))) = {
            let input = reader.fill_buf()?;
            let start_len = input.len();
            let r = tuple((take_while1(is_valid_op_char), control_args))(input);
            match r {
                Ok((input, (op, args))) => Some((input, start_len, (op, args))),
                Err(Incomplete(_)) => None,
                _ => return Err(parse_error()),
            }
        } {
            (input, start_len, (op, args))
        } else {
            reader.read_until(b'\n', &mut buf)?;
            let r = tuple((take_while1(is_valid_op_char), control_args))(&mut buf);
            let (input, (op, args)) = if let Ok((input, (op, args))) = r {
                (input, (op, args))
            } else {
                // Check for EOF, this happens on close of connection.
                if buf.len() == 0 {
                    return Ok(ControlOp::EOF);
                }
                return Err(parse_error());
            };
            (input, 0, (op, args))
        }
    };

    let op = match op {
        MSG => parse_msg_args(args)?,
        INFO => parse_info(args)?,
        PING => ControlOp::Ping,
        PONG => ControlOp::Pong,
        ERR => parse_err(args),
        _ => ControlOp::Unknown(String::from_utf8_lossy(op).to_string()),
    };

    // Make sure to consume the bytes from the underlying buffer.
    let stop_len = input.len();
    reader.consume(start_len - stop_len);

    Ok(op)
}

fn parse_msg_args(args: &[u8]) -> io::Result<ControlOp> {
    let a = String::from_utf8_lossy(args);
    // subject sid <reply> msg_len
    // TODO(dlc) - convert to nom.
    let args: Vec<&str> = a.split(' ').collect();
    let (subject, len_index, reply) = match args.len() {
        3 => (args[0], 2, None),
        4 => (args[0], 3, Some(args[2].to_owned())),
        _ => return Err(parse_error()),
    };
    let sid = if let Ok(sid) = usize::from_str(args[1]) {
        sid
    } else {
        return Err(parse_error());
    };
    let mlen = if let Ok(mlen) = u32::from_str(args[len_index]) {
        mlen
    } else {
        return Err(parse_error());
    };
    let m = MsgArgs {
        subject: subject.to_owned(),
        reply,
        //        data: Vec::with_capacity(mlen as usize),
        mlen,
        sid,
    };
    Ok(ControlOp::Msg(m))
}

fn parse_error() -> Error {
    Error::new(ErrorKind::InvalidInput, "parsing error")
}

fn parse_err(args: &[u8]) -> ControlOp {
    let err_description = String::from_utf8_lossy(args);
    let err_description = err_description.trim_matches('\'');
    ControlOp::Err(err_description.to_string())
}

pub(crate) fn expect_info(reader: &mut BufReader<TcpStream>) -> io::Result<ServerInfo> {
    let op = parse_control_op(reader)?;

    if let ControlOp::Info(info) = op {
        Ok(info)
    } else {
        Err(Error::new(ErrorKind::Other, "INFO proto not found"))
    }
}

const CRLF: &str = "\r\n";

#[inline]
fn control_args(input: &[u8]) -> IResult<&[u8], &[u8]> {
    let (remainder, (_, args, _)) = tuple((take_while(is_space), take_until(CRLF), crlf))(input)?;
    Ok((remainder, args))
}

fn parse_info(input: &[u8]) -> io::Result<ControlOp> {
    let info = serde_json::from_slice(input)?;
    Ok(ControlOp::Info(info))
}

#[derive(Debug)]
pub struct MsgArgs {
    subject: String,
    reply: Option<String>,
    mlen: u32,
    sid: usize,
}

#[derive(Debug)]
pub(crate) enum ControlOp {
    Msg(MsgArgs),
    Info(ServerInfo),
    Ping,
    Pong,
    EOF,
    Err(String),
    Unknown(String),
}
