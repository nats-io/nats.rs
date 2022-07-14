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

#![allow(dead_code)]
use std::io::{BufRead, BufReader};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command};
use std::{env, fs};
use std::{thread, time::Duration};

use lazy_static::lazy_static;
use rand::Rng;
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

    pub fn client_port(&self) -> u16 {
        let addr = self.client_addr();
        let mut r = BufReader::with_capacity(1024, TcpStream::connect(addr).unwrap());
        let mut line = String::new();
        r.read_line(&mut line).expect("did not receive INFO");
        let si = json::parse(&line["INFO".len()..]).unwrap();
        si["port"].as_u16().expect("could not parse port")
    }

    // Allow user/pass override.
    pub fn client_url_with(&self, user: &str, pass: &str) -> String {
        use url::Url;
        let mut url = Url::parse(&self.client_url()).expect("could not parse");
        url.set_username(user).ok();
        url.set_password(Some(pass)).ok();
        url.as_str().to_string()
    }

    // Allow token override.
    pub fn client_url_with_token(&self, token: &str) -> String {
        use url::Url;
        let mut url = Url::parse(&self.client_url()).expect("could not parse");
        url.set_username(token).ok();
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

pub fn run_server(cfg: &str) -> Server {
    run_server_with_port(cfg, None)
}

pub fn is_port_available(port: usize) -> bool {
    TcpListener::bind(("127.0.0.1", port.try_into().unwrap())).is_ok()
}

/// Start a NATS Cluster with optional config for each node.
pub fn run_cluster(cfg: Vec<&str>, jetstream: bool) -> Cluster {
    let port = rand::thread_rng().gen_range(3000..50_000);
    let ports = vec![port, port + 100, port + 200];

    let ports = ports
        .iter()
        .map(|port| {
            let mut new_port = *port;
            while !is_port_available(new_port) || !is_port_available(new_port + 1) {
                new_port = rand::thread_rng().gen_range(2000..50_000);
            }
            new_port
        })
        .collect::<Vec<usize>>();
    let cluster = vec![port + 1, port + 101, port + 201];

    let s1 = run_cluster_node_with_port(
        cfg[0],
        Some(ports[0].to_string().as_str()),
        vec![cluster[1], cluster[2]],
        "node1".to_string(),
        "cluster".to_string(),
        cluster[0],
        jetstream,
    );
    let s2 = run_cluster_node_with_port(
        cfg[1],
        Some(ports[1].to_string().as_str()),
        vec![cluster[0], cluster[2]],
        "node2".to_string(),
        "cluster".to_string(),
        cluster[1],
        jetstream,
    );
    let s3 = run_cluster_node_with_port(
        cfg[2],
        Some(ports[2].to_string().as_str()),
        vec![cluster[0], cluster[1]],
        "node3".to_string(),
        "cluster".to_string(),
        cluster[2],
        jetstream,
    );
    Cluster {
        servers: vec![s1, s2, s3],
    }
}

pub struct Cluster {
    servers: Vec<Server>,
}

/// Starts a local NATS server with the given config that gets stopped and cleaned up on drop.
pub fn run_server_with_port(cfg: &str, port: Option<&str>) -> Server {
    let id = nuid::next();
    let logfile = env::temp_dir().join(format!("nats-server-{}.log", id));
    let store_dir = env::temp_dir().join(format!("store-dir-{}", id));
    let pidfile = env::temp_dir().join(format!("nats-server-{}.pid", id));

    // Always use dynamic ports so tests can run in parallel.
    // Create env for a storage directory for jetstream.
    let mut cmd = Command::new("nats-server");
    cmd.arg("--store_dir")
        .arg(store_dir.as_path().to_str().unwrap())
        .arg("-p");
    match port {
        Some(port) => cmd.arg(port),
        None => cmd.arg("-1"),
    };
    cmd.arg("-l")
        .arg(logfile.as_os_str())
        .arg("-P")
        .arg(pidfile.as_os_str());

    if !cfg.is_empty() {
        cmd.arg("-c").arg(cfg);
    }

    let child = cmd.spawn().unwrap();

    Server {
        child,
        logfile,
        pidfile,
    }
}

fn run_cluster_node_with_port(
    cfg: &str,
    port: Option<&str>,
    routes: Vec<usize>,
    name: String,
    cluster_name: String,
    cluster: usize,
    jetstream: bool,
) -> Server {
    let id = nuid::next();
    let logfile = env::temp_dir().join(format!("nats-server-{}.log", id));
    let store_dir = env::temp_dir().join(format!("store-dir-{}", id));
    let pidfile = env::temp_dir().join(format!("nats-server-{}.pid", id));

    // Always use dynamic ports so tests can run in parallel.
    // Create env for a storage directory for jetstream.
    let mut cmd = Command::new("nats-server");
    cmd.arg("--store_dir")
        .arg(store_dir.as_path().to_str().unwrap())
        .arg("-p");
    match port {
        Some(port) => cmd.arg(port),
        None => cmd.arg("-1"),
    };
    cmd.arg("-l")
        .arg(logfile.as_os_str())
        .arg("-P")
        .arg(pidfile.as_os_str())
        .arg("--routes")
        .arg(
            routes
                .iter()
                .map(|r| format!("nats://127.0.0.1:{}", r))
                .collect::<Vec<String>>()
                .join(","),
        )
        .arg("--cluster")
        .arg(format!("nats://127.0.0.1:{}", cluster))
        .arg("--cluster_name")
        .arg(cluster_name)
        .arg("-n")
        .arg(name);

    if jetstream {
        cmd.arg("--js");
    }
    if !cfg.is_empty() {
        cmd.arg("-c").arg(cfg);
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

#[cfg(test)]
mod tests {

    use crate::run_cluster;

    #[tokio::test]
    async fn cluster_with_js() {
        let cluster = run_cluster(vec!["", "", ""], true);

        let client = async_nats::connect(cluster.servers[0].client_url())
            .await
            .unwrap();

        let jetstream = async_nats::jetstream::new(client);

        let stream = jetstream
            .create_stream(async_nats::jetstream::stream::Config {
                name: "replicated".to_string(),
                num_replicas: 3,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(stream.info.config.num_replicas, 3);

        jetstream
            .create_stream(async_nats::jetstream::stream::Config {
                name: "replicated_too_much".to_string(),
                num_replicas: 5,
                ..Default::default()
            })
            .await
            .expect_err("this should error as its only 3 nodes cluster");
        jetstream.delete_stream("replicated").await.unwrap();
    }

    #[tokio::test]
    async fn cluster_without_js() {
        use futures::StreamExt;
        let cluster = run_cluster(vec!["", "", ""], true);

        let client = async_nats::connect(cluster.servers[0].client_url())
            .await
            .unwrap();
        let mut subscribe = client.subscribe("foo".into()).await.unwrap();
        client.publish("foo".into(), "bar".into()).await.unwrap();
        subscribe.next().await.unwrap();
    }
}
