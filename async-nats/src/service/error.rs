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

use std::fmt::Display;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize)]
pub struct Error(pub usize, pub String);

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "service request error code: {}, status: {}",
            self.0, self.1
        )
    }
}

impl Serialize for Error {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serde_json::to_string(&ErrorDT::from(self.clone()))
            .unwrap()
            .serialize(serializer)
    }
}
impl std::error::Error for Error {}

#[derive(Serialize, Deserialize)]
struct ErrorDT {
    status: String,
    code: usize,
}

impl From<Error> for ErrorDT {
    fn from(value: Error) -> Self {
        ErrorDT {
            code: value.0,
            status: value.1,
        }
    }
}
