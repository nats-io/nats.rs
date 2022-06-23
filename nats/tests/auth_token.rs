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

#[test]
fn token_auth() {
    let s = nats_server::run_server("tests/configs/token_auth.conf");

    assert!(nats::connect(&s.client_url()).is_err());

    assert!(nats::Options::with_token("some-auth-token")
        .connect(&s.client_url())
        .is_ok());

    assert!(nats::connect(&s.client_url_with_token("some-auth-token")).is_ok());

    // Check override.
    assert!(nats::Options::with_token("bad-auth-token")
        .connect(&s.client_url_with_token("some-auth-token"))
        .is_ok());
}
