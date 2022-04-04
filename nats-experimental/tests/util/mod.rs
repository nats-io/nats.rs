// Copyright 2020-2022 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::{BufRead, BufReader};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::{env, fs};
use std::{thread, time::Duration};

use lazy_static::lazy_static;
use nats::jetstream::{JetStream, JetStreamOptions};
use nats::Connection;
use regex::Regex;

pub struct Server {
    child: Child,
    logfile: PathBuf,
    pidfile: PathBuf,
}

lazy_static! {
    static ref SD_RE: Regex = Regex::new(r#".+\sStore Directory:\s+"([^"]+)""#).unwrap();
    static ref CLIENT_RE: Regex = Regex::new(r#".+\sclient connections on\s+(\S+)"#).unwrap();
}

impl Drop for Server {
    fn drop(&mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
        // Remove log if present.
        if let Ok(log) = fs::read_to_string(self.logfile.as_os_str()) {
            // Check if we had JetStream running and if so cleanup the storage directory.
            if let Some(caps) = SD_RE.captures(&log) {
                let sd = caps.get(1).map_or("", |m| m.as_str());
                fs::remove_dir_all(sd).ok();
            }
            // Remove Logfile.
            fs::remove_file(self.logfile.as_os_str()).ok();
        }
    }
}

impl Server {
    // Grab client url.
    // Helpful when dynamically allocating ports with -1.
    pub fn client_url(&self) -> String {
        let addr = self.client_addr();
        let mut r = BufReader::with_capacity(1024, TcpStream::connect(addr).unwrap());
        let mut line = String::new();
        r.read_line(&mut line).expect("did not receive INFO");
        let si = json::parse(&line["INFO".len()..]).unwrap();
        let port = si["port"].as_u16().expect("could not parse port");
        let mut scheme = "nats://";
        if si["tls_required"].as_bool().unwrap_or(false) {
            scheme = "tls://";
        }
        format!("{}127.0.0.1:{}", scheme, port)
    }

    // Allow user/pass override.
    pub fn client_url_with(&self, user: &str, pass: &str) -> String {
        use url::Url;
        let mut url = Url::parse(&self.client_url()).expect("could not parse");
        url.set_username(user).ok();
        url.set_password(Some(pass)).ok();
        url.as_str().to_string()
    }

    // Grab client addr from logs.
    fn client_addr(&self) -> String {
        // We may need to wait for log to be present.
        // Wait up to 2s. (20 * 100ms)
        for _ in 0..20 {
            match fs::read_to_string(self.logfile.as_os_str()) {
                Ok(l) => {
                    if let Some(cre) = CLIENT_RE.captures(&l) {
                        return cre.get(1).unwrap().as_str().replace("0.0.0.0", "127.0.0.1");
                    } else {
                        thread::sleep(Duration::from_millis(250));
                    }
                }
                _ => thread::sleep(Duration::from_millis(250)),
            }
        }
        panic!("no client addr info");
    }

    pub fn client_pid(&self) -> usize {
        String::from_utf8(fs::read(self.pidfile.clone()).unwrap())
            .unwrap()
            .parse()
            .unwrap()
    }
}

pub fn set_lame_duck_mode(s: &Server) {
    let mut cmd = Command::new("nats-server");
    cmd.arg("--signal")
        .arg(format!("ldm={}", s.client_pid()))
        .spawn()
        .unwrap();
}

/// Starts a local NATS server with the given config that gets stopped and cleaned up on drop.
pub fn run_server(cfg: &str) -> Server {
    let id = nuid::next();
    let logfile = env::temp_dir().join(format!("nats-server-{}.log", id));
    let store_dir = env::temp_dir().join(format!("store-dir-{}", id));
    let pidfile = env::temp_dir().join(format!("nats-server-{}.pid", id));

    // Always use dynamic ports so tests can run in parallel.
    // Create env for a storage directory for jetstream.
    let mut cmd = Command::new("nats-server");
    cmd.arg("--store_dir")
        .arg(store_dir.as_path().to_str().unwrap())
        .arg("-p")
        .arg("-1")
        .arg("-l")
        .arg(logfile.as_os_str())
        .arg("-P")
        .arg(pidfile.as_os_str());

    if cfg != "" {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        cmd.arg("-c").arg(path.join(cfg));
    }

    let child = cmd.spawn().unwrap();

    Server {
        child,
        logfile,
        pidfile,
    }
}

/// Starts a local basic NATS server that gets stopped and cleaned up on drop.
pub fn run_basic_server() -> Server {
    run_server("")
}

// Helper function to return server and client.
pub fn run_basic_jetstream() -> (Server, Connection, JetStream) {
    let s = run_server("tests/configs/jetstream.conf");
    let nc = nats::connect(&s.client_url()).unwrap();
    let js = JetStream::new(nc.clone(), JetStreamOptions::default());

    (s, nc, js)
}
