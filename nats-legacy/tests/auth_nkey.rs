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

use std::io;

mod util;
pub use util::*;

#[test]
fn basic_nkey_auth() -> io::Result<()> {
    let s = util::run_server("tests/configs/nkey.conf");

    let nkey = "UAMMBNV2EYR65NYZZ7IAK5SIR5ODNTTERJOBOF4KJLMWI45YOXOSWULM";
    let seed = "SUANQDPB2RUOE4ETUA26CNX7FUKE5ZZKFCQIIW63OX225F2CO7UEXTM7ZY";
    let kp = nkeys::KeyPair::from_seed(seed).unwrap();

    nats::Options::with_nkey(nkey, move |nonce| kp.sign(nonce).unwrap())
        .connect(&s.client_url())?;

    Ok(())
}
