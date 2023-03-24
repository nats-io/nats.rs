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

use crate::jetstream::stream::Info;

/// Represents status information about a key value store bucket
#[derive(Debug)]
pub struct Status {
    pub info: Info,
    pub bucket: String,
}

impl Status {
    /// The name of the bucket
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// How many messages are in the bucket, including historical values
    pub fn values(&self) -> u64 {
        self.info.state.messages
    }

    /// Configured history kept per key
    pub fn history(&self) -> i64 {
        self.info.config.max_messages_per_subject
    }

    /// How long the bucket keeps values for
    pub fn max_age(&self) -> Duration {
        self.info.config.max_age
    }
}
