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

pub use nats_server::*;

#[test]
#[should_panic(expected = "no responders")]
fn no_responders() {
    let s = nats_server::run_basic_server();
    let nc = nats::connect(&s.client_url()).expect("could not connect");
    nc.request("nobody-home", "hello").unwrap();
}
