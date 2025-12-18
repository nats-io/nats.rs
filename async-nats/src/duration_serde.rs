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

//! Serde helpers for std::time::Duration as nanoseconds
//! Works with both chrono and time crate features

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::time::Duration;

/// Serialize a Duration as nanoseconds (as a u64)
pub(crate) fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    duration.as_nanos().serialize(serializer)
}

/// Deserialize a Duration from nanoseconds (as a u64 or i64)
pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let nanos = i64::deserialize(deserializer)?;
    Ok(Duration::from_nanos(nanos.max(0) as u64))
}

/// Module for Option<Duration> serialization
pub(crate) mod option {
    use super::*;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub(crate) fn serialize<S>(
        duration: &Option<Duration>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match duration {
            Some(d) => Some(d.as_nanos()).serialize(serializer),
            None => None::<u128>.serialize(serializer),
        }
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
    where
        D: Deserializer<'de>,
    {
        match Option::<i64>::deserialize(deserializer)? {
            Some(nanos) => Ok(Some(Duration::from_nanos(nanos.max(0) as u64))),
            None => Ok(None),
        }
    }
}

/// Module for Vec<Duration> serialization
pub(crate) mod vec {
    use super::*;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub(crate) fn serialize<S>(durations: &Vec<Duration>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let nanos: Vec<u128> = durations.iter().map(|d| d.as_nanos()).collect();
        nanos.serialize(serializer)
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Vec<Duration>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let nanos_vec = Vec::<i64>::deserialize(deserializer)?;
        Ok(nanos_vec
            .into_iter()
            .map(|nanos| Duration::from_nanos(nanos.max(0) as u64))
            .collect())
    }
}
