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

use crate::{Connection, ToServerAddrs};
use std::{fmt, marker::PhantomData, path::PathBuf};
use tokio::io;

/// Connect options.
#[derive(Clone)]
pub struct Options {
    // pub(crate) auth: AuthStyle,
    pub(crate) name: Option<String>,
    pub(crate) no_echo: bool,
    pub(crate) retry_on_failed_connect: bool,
    pub(crate) max_reconnects: Option<usize>,
    pub(crate) reconnect_buffer_size: usize,
    pub(crate) tls_required: bool,
    pub(crate) certificates: Vec<PathBuf>,
    pub(crate) client_cert: Option<PathBuf>,
    pub(crate) client_key: Option<PathBuf>,
    pub(crate) tls_client_config: Option<crate::rustls::ClientConfig>,
}

impl fmt::Debug for Options {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_map()
            .entry(&"name", &self.name)
            .entry(&"no_echo", &self.no_echo)
            .entry(&"retry_on_failed_connect", &self.retry_on_failed_connect)
            .entry(&"reconnect_buffer_size", &self.reconnect_buffer_size)
            .entry(&"max_reconnects", &self.max_reconnects)
            .entry(&"tls_required", &self.tls_required)
            .entry(&"certificates", &self.certificates)
            .entry(&"client_cert", &self.client_cert)
            .entry(&"client_key", &self.client_key)
            .entry(&"tls_client_config", &"XXXXXXXX")
            .finish()
    }
}

impl Default for Options {
    fn default() -> Options {
        Options {
            name: None,
            no_echo: false,
            retry_on_failed_connect: false,
            reconnect_buffer_size: 8 * 1024 * 1024,
            max_reconnects: Some(60),
            tls_required: false,
            certificates: Vec::new(),
            client_cert: None,
            client_key: None,
            tls_client_config: None,
        }
    }
}

impl Options {
    pub fn new() -> Self {
        Options::default()
    }
    pub async fn connect<A: ToServerAddrs>(&mut self, addrs: A) -> io::Result<Connection> {
        Connection::connect_with_options(addrs, self.to_owned()).await
    }

    pub fn add_root_certificates(&mut self, path: PathBuf) -> &mut Options {
        self.certificates = vec![path];
        self
    }

    pub fn add_client_certificates(&mut self, cert: PathBuf, key: PathBuf) -> &mut Options {
        self.client_cert = Some(cert);
        self.client_key = Some(key);
        self
    }
    pub fn require_tls(&mut self, is_required: bool) -> &mut Options {
        self.tls_required = is_required;
        self
    }
}

// struct TLSOptions<CC = False, CK = False> {
//     client_cert_set: PhantomData<CC>,
//     client_key_set: PhantomData<CK>,

//     pub(crate) tls_required: bool,
//     pub(crate) certificates: Vec<PathBuf>,
//     pub(crate) client_cert: Option<PathBuf>,
//     pub(crate) client_key: Option<PathBuf>,
// }

// impl<CC, CK> TLSOptions<CC, CK> {
//     fn new() -> Self {
//         Default::default()
//     }
//     pub fn add_root_certificates(&mut self, path: PathBuf) -> &mut TLSOptions {
//         self.certificates = vec![path];
//         self
//     }

//     pub fn add_client_certificates(&mut self, cert: PathBuf, key: PathBuf) -> &mut TLSOptions {
//         self.client_cert = Some(cert);
//         self.client_key = Some(key);
//         self
//     }
//     pub fn require_tls(&mut self, is_required: bool) -> &mut TLSOptions {
//         self.tls_required = is_required;
//         self
//     }
// }

// impl Default for TLSOptions {
//     fn default() -> Self {
//         TLSOptions {
//             tls_required: true,
//             certificates: Vec::new(),
//             client_key: None,
//             client_cert: None,
//         }
//     }
// }

pub struct True;
pub struct False;
trait Complete {}

impl Complete for True {}
impl Complete for False {}
