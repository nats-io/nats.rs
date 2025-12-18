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

//! Compatibility layer for datetime types that supports both time and chrono crates

/// Macro to define the DateTime type based on feature flags
#[macro_export]
macro_rules! datetime_type {
    () => {
        #[cfg(feature = "chrono-crate")]
        chrono::DateTime<chrono::FixedOffset>

        #[cfg(feature = "time-crate")]
        time::OffsetDateTime
    };
}

// Re-export the appropriate crate's types
#[cfg(feature = "chrono-crate")]
pub(crate) use chrono::{DateTime, FixedOffset, Utc};

#[cfg(feature = "time-crate")]
pub(crate) use time::{Duration as TimeDuration, OffsetDateTime};

// Provide a unified type alias
#[cfg(feature = "chrono-crate")]
pub(crate) type DateTimeType = DateTime<FixedOffset>;

#[cfg(feature = "time-crate")]
pub(crate) type DateTimeType = OffsetDateTime;

/// Helper to get current UTC time
#[cfg(feature = "chrono-crate")]
#[inline]
pub(crate) fn now_utc() -> DateTimeType {
    Utc::now().fixed_offset()
}

#[cfg(feature = "time-crate")]
#[inline]
pub(crate) fn now_utc() -> DateTimeType {
    OffsetDateTime::now_utc()
}

/// Parse RFC3339 datetime string
#[cfg(feature = "chrono-crate")]
#[inline]
pub(crate) fn parse_rfc3339(
    s: &str,
) -> Result<DateTimeType, Box<dyn std::error::Error + Send + Sync>> {
    Ok(DateTime::parse_from_rfc3339(s)?)
}

#[cfg(feature = "time-crate")]
#[inline]
pub(crate) fn parse_rfc3339(
    s: &str,
) -> Result<DateTimeType, Box<dyn std::error::Error + Send + Sync>> {
    use time::format_description::well_known::Rfc3339;
    Ok(OffsetDateTime::parse(s, &Rfc3339)?)
}

/// Create datetime from unix timestamp in nanoseconds
#[cfg(feature = "chrono-crate")]
#[inline]
pub(crate) fn from_timestamp_nanos(nanos: i64) -> DateTimeType {
    use chrono::TimeZone;
    FixedOffset::east_opt(0).unwrap().timestamp_nanos(nanos)
}

#[cfg(feature = "time-crate")]
#[inline]
pub(crate) fn from_timestamp_nanos(nanos: i64) -> DateTimeType {
    OffsetDateTime::from_unix_timestamp_nanos(nanos as i128).unwrap()
}

/// Serde module for DateTime serialization
#[cfg(feature = "chrono-crate")]
pub(crate) mod datetime_serde {
    use super::*;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub(crate) fn serialize<S>(dt: &DateTimeType, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        dt.serialize(serializer)
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<DateTimeType, D::Error>
    where
        D: Deserializer<'de>,
    {
        DateTimeType::deserialize(deserializer)
    }

    pub(crate) mod option {
        use super::*;

        pub(crate) fn serialize<S>(
            dt: &Option<DateTimeType>,
            serializer: S,
        ) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            dt.serialize(serializer)
        }

        pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Option<DateTimeType>, D::Error>
        where
            D: Deserializer<'de>,
        {
            Option::<DateTimeType>::deserialize(deserializer)
        }
    }
}

#[cfg(feature = "time-crate")]
pub(crate) mod datetime_serde {
    use super::*;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use time::serde::rfc3339;

    pub(crate) fn serialize<S>(dt: &DateTimeType, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        rfc3339::serialize(dt, serializer)
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<DateTimeType, D::Error>
    where
        D: Deserializer<'de>,
    {
        rfc3339::deserialize(deserializer)
    }

    pub(crate) mod option {
        use super::*;

        pub(crate) fn serialize<S>(
            dt: &Option<DateTimeType>,
            serializer: S,
        ) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            rfc3339::option::serialize(dt, serializer)
        }

        pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Option<DateTimeType>, D::Error>
        where
            D: Deserializer<'de>,
        {
            rfc3339::option::deserialize(deserializer)
        }
    }
}
