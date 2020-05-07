use std::io;
use std::net::TcpStream;
use std::thread;

use futures::{channel::mpsc, io::BufReader, prelude::*};
use piper::Arc;
use smol::{block_on, Async, Timer};

use crate::{connect::ConnectInfo, ServerInfo};

pub struct Conn {
    pub_chan: mpsc::UnboundedSender<(String, Vec<u8>)>,
    thread: thread::JoinHandle<io::Result<()>>,
}

impl Conn {
    pub fn connect(url: &str) -> io::Result<Conn> {
        let url = url.to_string();
        let (pub_sender, pub_receiver) = mpsc::unbounded();
        Ok(Conn {
            thread: thread::spawn(move || smol::run(client(&url, pub_receiver))),
            pub_chan: pub_sender,
        })
    }

    pub fn publish(&mut self, subject: &str, msg: impl AsRef<[u8]>) -> io::Result<()> {
        let subject = subject.to_owned();
        let msg = msg.as_ref().to_owned();
        block_on(self.pub_chan.send((subject, msg))).expect("disconnected");
        Ok(())
    }
}

async fn client(
    url: &str,
    mut pub_chan: mpsc::UnboundedReceiver<(String, Vec<u8>)>,
) -> io::Result<()> {
    let stream = Arc::new(Async::<TcpStream>::connect(url).await?);
    let reader = BufReader::new(stream.clone());
    let mut lines = reader.lines();
    let mut writer = stream;

    let line = lines.next().await.expect("expected INFO line")?;
    let line = line.trim_start_matches("INFO ");
    let server_info: ServerInfo = serde_json::from_slice(line.as_bytes()).expect("cannot parse");
    dbg!(server_info);

    let connect_info = ConnectInfo {
        tls_required: false,
        name: None,
        pedantic: false,
        verbose: false,
        lang: crate::LANG.to_string(),
        version: crate::VERSION.to_string(),
        user: None,
        pass: None,
        auth_token: None,
        user_jwt: None,
        signature: None,
        echo: true,
    };

    let op = format!(
        "CONNECT {}\r\nPING\r\n",
        serde_json::to_string(&connect_info)?
    );
    writer.write_all(op.as_bytes()).await?;

    loop {
        futures::select! {
            line = lines.next().fuse() => {
                let line = line.expect("disconnected")?;
                dbg!(&line);

                if line == "PING" {
                    writer.write_all(b"PONG\r\n").await?;
                } else if line == "PONG" {
                    // TODO
                } else if line.starts_with("MSG ") {
                    // TODO
                } else {
                    panic!("unknown command: {}", line);
                }
            }

            msg = pub_chan.next().fuse() => {
                dbg!(&msg);
                let (subject, msg) = msg.expect("disconnected");
                writer.write_all(format!("PUB {} {}\r\n", subject, msg.len()).as_bytes()).await?;
                writer.write_all(&msg).await?;
                writer.write_all(b"\r\n").await?;
            }
        }
    }
}
