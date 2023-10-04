// Copyright 2020-2023 The NATS Authors
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

//! A Core NATS message.
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::header::HeaderMap;
use crate::status::StatusCode;
use crate::subject::Subject;

/// A Core NATS message.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message {
    /// Subject to which message is published to.
    pub subject: Subject,
    /// Optional reply subject to which response can be published by [crate::Subscriber].
    /// Used for request-response pattern with [crate::Client::request].
    pub reply: Option<Subject>,
    /// Payload of the message. Can be any arbitrary data format.
    pub payload: Bytes,
    /// Optional headers.
    pub headers: Option<HeaderMap>,
    /// Optional Status of the message. Used mostly for internal handling.
    pub status: Option<StatusCode>,
    /// Optional [status][crate::Message::status] description.
    pub description: Option<String>,

    pub length: usize,
}
