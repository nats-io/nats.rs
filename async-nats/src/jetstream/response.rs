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

use serde::Deserialize;

/// An error description returned in a response to a jetstream request.
#[derive(Debug, Deserialize)]
pub struct Error {
    /// Code
    pub code: u64,

    /// Description
    pub description: String,
}

/// A response returned from a request to jetstream.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum Response<T> {
    Err { error: Error },
    Ok(T),
}
