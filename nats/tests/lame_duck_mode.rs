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

use std::time::Duration;

use crossbeam_channel::bounded;

#[test]
#[cfg_attr(target_os = "windows", ignore)]
fn lame_duck_mode() {
    let (ltx, lrx) = bounded(1);

    let s = nats_server::run_basic_server();
    let nc = nats::Options::new()
        .lame_duck_callback(move || ltx.send(true).unwrap())
        .connect(s.client_url().as_str())
        .expect("could not connect to the server");
    let _sub = nc.subscribe("foo").unwrap();
    nats_server::set_lame_duck_mode(&s);
    let r = lrx.recv_timeout(Duration::from_millis(500));
    assert!(r.is_ok(), "expected lame duck response, got nothing");
}
