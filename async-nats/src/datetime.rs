// Copyright 2020-2026 The NATS Authors
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

//! Datetime abstraction over the `time` and `chrono` backends.
//!
//! `async-nats` exposes datetimes on its JetStream and Service APIs through the
//! [`DateTime`] alias. The concrete type is selected by the `chrono` feature:
//!
//! - By default, [`DateTime`] is `time::OffsetDateTime`.
//! - With the `chrono` feature enabled, [`DateTime`] is
//!   `chrono::DateTime<chrono::Utc>`.
//!
//! Note: the `chrono` feature selects the backend for the whole build. Because
//! Cargo unifies features across the dependency graph, enabling `chrono`
//! anywhere in the graph switches the public type to chrono for every consumer
//! of `async-nats` in that build. If you depend on the default `time` backend,
//! be aware a transitive dependency that enables `async-nats/chrono` will
//! change [`DateTime`] to the chrono type.
//!
//! The chrono backend also stores each instant as an `i64` nanosecond count
//! (range roughly the years 1678 to 2262), narrower than the `time` backend;
//! under chrono, [`from_nanos`] returns an error for values outside that range.

/// The datetime type used across the JetStream and Service APIs.
///
/// Defaults to `time::OffsetDateTime`; becomes `chrono::DateTime<chrono::Utc>`
/// when the `chrono` feature is enabled. Enabling `chrono` anywhere in the
/// dependency graph changes this type for the whole build; see the [module
/// docs](self) for the feature-unification caveat.
#[cfg(not(feature = "chrono"))]
pub type DateTime = time::OffsetDateTime;

/// The datetime type used across the JetStream and Service APIs.
///
/// Defaults to `time::OffsetDateTime`; becomes `chrono::DateTime<chrono::Utc>`
/// when the `chrono` feature is enabled. Enabling `chrono` anywhere in the
/// dependency graph changes this type for the whole build; see the [module
/// docs](self) for the feature-unification caveat.
#[cfg(feature = "chrono")]
pub type DateTime = chrono::DateTime<chrono::Utc>;

/// Parse an RFC 3339 timestamp into a [`DateTime`] (normalized to UTC).
#[cfg(not(feature = "chrono"))]
pub fn parse_rfc3339(s: &str) -> Result<DateTime, crate::Error> {
    time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
        // Normalize to UTC so both backends agree on the emitted offset (`Z`).
        .map(|dt| dt.to_offset(time::UtcOffset::UTC))
        .map_err(|e| e.into())
}

/// Parse an RFC 3339 timestamp into a [`DateTime`] (normalized to UTC).
#[cfg(feature = "chrono")]
pub fn parse_rfc3339(s: &str) -> Result<DateTime, crate::Error> {
    chrono::DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .map_err(|e| e.into())
}

/// Convert a Unix timestamp in nanoseconds to a [`DateTime`].
#[cfg(not(feature = "chrono"))]
pub fn from_nanos(nanos: i128) -> Result<DateTime, crate::Error> {
    time::OffsetDateTime::from_unix_timestamp_nanos(nanos).map_err(|e| e.into())
}

/// Convert a Unix timestamp in nanoseconds to a [`DateTime`].
///
/// Note: the `chrono` backend represents the nanosecond timestamp as an `i64`,
/// so values outside roughly the years 1678 to 2262 are rejected. The `time`
/// backend accepts the full `i128` range.
#[cfg(feature = "chrono")]
pub fn from_nanos(nanos: i128) -> Result<DateTime, crate::Error> {
    let nanos_i64: i64 = nanos.try_into().map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "nanoseconds value out of range for the chrono backend (supports ~1678 to 2262)",
        )
    })?;
    // Use Euclidean division so that negative (pre-1970) timestamps split into a
    // non-negative sub-second component, matching the `time` backend's behavior.
    let secs = nanos_i64.div_euclid(1_000_000_000);
    let nsecs = nanos_i64.rem_euclid(1_000_000_000) as u32;
    chrono::DateTime::from_timestamp(secs, nsecs).ok_or_else(|| {
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "invalid timestamp",
        )) as crate::Error
    })
}

/// Return the current time as a UTC [`DateTime`].
#[cfg(not(feature = "chrono"))]
pub fn now() -> DateTime {
    time::OffsetDateTime::now_utc()
}

/// Return the current time as a UTC [`DateTime`].
#[cfg(feature = "chrono")]
pub fn now() -> DateTime {
    chrono::Utc::now()
}

// Internal serde adapter backing the `#[serde(with = "rfc3339")]` field
// attributes. Kept crate-private because its public shape differs per backend
// (a re-export under `time`, a hand-written module under `chrono`).
#[cfg(not(feature = "chrono"))]
pub(crate) use time::serde::rfc3339;

// Some serialize/deserialize fns here are unused in feature combinations that
// enable `chrono` without a field that consumes the adapter (e.g. `chrono` alone,
// or `chrono` + only `service`, which never uses the `option` variant). The time
// arm above is a re-export and is dead-code-exempt; this hand-written module is not.
#[cfg(feature = "chrono")]
#[allow(dead_code)]
pub(crate) mod rfc3339 {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    pub(crate) fn serialize<S>(
        dt: &chrono::DateTime<chrono::Utc>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        Serialize::serialize(dt, serializer)
    }
    pub(crate) fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<chrono::DateTime<chrono::Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Deserialize::deserialize(deserializer)
    }

    pub(crate) mod option {
        use serde::{Deserialize, Deserializer, Serialize, Serializer};
        pub(crate) fn serialize<S>(
            dt: &Option<chrono::DateTime<chrono::Utc>>,
            serializer: S,
        ) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            Serialize::serialize(dt, serializer)
        }
        pub(crate) fn deserialize<'de, D>(
            deserializer: D,
        ) -> Result<Option<chrono::DateTime<chrono::Utc>>, D::Error>
        where
            D: Deserializer<'de>,
        {
            Deserialize::deserialize(deserializer)
        }
    }
}

/// Add a [`std::time::Duration`] to a [`DateTime`].
///
/// Returns an error if the duration is too large to be represented by the
/// duration type of the active backend.
#[cfg(not(feature = "chrono"))]
pub fn add_std_duration(
    dt: DateTime,
    duration: std::time::Duration,
) -> Result<DateTime, crate::Error> {
    let time_duration = time::Duration::try_from(duration)?;
    dt.checked_add(time_duration).ok_or_else(|| {
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "datetime overflow",
        )) as crate::Error
    })
}

/// Add a [`std::time::Duration`] to a [`DateTime`].
///
/// Returns an error if the duration is too large to be represented by the
/// duration type of the active backend.
#[cfg(feature = "chrono")]
pub fn add_std_duration(
    dt: DateTime,
    duration: std::time::Duration,
) -> Result<DateTime, crate::Error> {
    let chrono_duration = chrono::Duration::from_std(duration)?;
    dt.checked_add_signed(chrono_duration).ok_or_else(|| {
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "datetime overflow",
        )) as crate::Error
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    struct Wrap(#[serde(with = "rfc3339")] DateTime);

    #[derive(Serialize, Deserialize, Default)]
    struct WrapOpt(
        #[serde(
            default,
            with = "rfc3339::option",
            skip_serializing_if = "Option::is_none"
        )]
        Option<DateTime>,
    );

    // A fixed instant with a full 9-significant-digit fraction:
    // 2023-01-02T03:04:05.123456789Z
    const FIXED_NANOS: i128 = 1_672_628_645_123_456_789;
    const FIXED_RFC3339: &str = "2023-01-02T03:04:05.123456789Z";

    // With all 9 fraction digits significant, `time` and `chrono` emit identical
    // bytes; this pins that case for whichever backend is built. (They diverge on
    // trailing zeros — see `rfc3339_trailing_zero_fractions_are_instant_equal`.)
    #[test]
    fn rfc3339_full_precision_is_byte_identical() {
        let dt = from_nanos(FIXED_NANOS).unwrap();
        let json = serde_json::to_string(&Wrap(dt)).unwrap();
        assert_eq!(json, format!("\"{FIXED_RFC3339}\""));
    }

    #[test]
    fn rfc3339_roundtrips() {
        let dt = from_nanos(FIXED_NANOS).unwrap();
        let json = serde_json::to_string(&Wrap(dt)).unwrap();
        let back: Wrap = serde_json::from_str(&json).unwrap();
        assert_eq!(serde_json::to_string(&back).unwrap(), json);
    }

    #[test]
    fn parse_rfc3339_matches_from_nanos() {
        let parsed = parse_rfc3339(FIXED_RFC3339).unwrap();
        let from_ts = from_nanos(FIXED_NANOS).unwrap();
        assert_eq!(parsed, from_ts);
    }

    // The NATS server emits timestamps via Go's RFC3339Nano, which trims trailing
    // zeros from the fractional part (and omits it entirely on a whole second).
    // Both backends must accept every such shape and decode to the same instant.
    #[test]
    fn parse_rfc3339_server_format_variants() {
        let cases: &[(&str, i128)] = &[
            // no fractional seconds
            ("2023-01-02T03:04:05Z", 1_672_628_645_000_000_000),
            // millisecond precision
            ("2023-01-02T03:04:05.123Z", 1_672_628_645_123_000_000),
            // microsecond precision
            ("2023-01-02T03:04:05.123456Z", 1_672_628_645_123_456_000),
            // full nanosecond precision
            ("2023-01-02T03:04:05.123456789Z", 1_672_628_645_123_456_789),
        ];
        for (s, nanos) in cases {
            let parsed = parse_rfc3339(s).unwrap_or_else(|e| panic!("parse {s:?}: {e}"));
            assert_eq!(
                parsed,
                from_nanos(*nanos).unwrap(),
                "instant mismatch for {s:?}"
            );
            // serde deserialize path must accept the same wire shapes.
            let de: Wrap = serde_json::from_str(&format!("\"{s}\""))
                .unwrap_or_else(|e| panic!("serde deserialize {s:?}: {e}"));
            assert_eq!(
                de.0,
                from_nanos(*nanos).unwrap(),
                "serde mismatch for {s:?}"
            );
        }
    }

    // The backends serialize trailing-zero fractions differently (e.g. `time`
    // emits `.12Z`, `chrono` emits `.120Z`), but both must re-serialize to a form
    // the other and the server accept, and both must round-trip to the same instant.
    #[test]
    fn rfc3339_trailing_zero_fractions_are_instant_equal() {
        for nanos in [
            1_672_628_645_120_000_000, // .12
            1_672_628_645_100_000_000, // .1
            1_672_628_645_000_000_000, // whole second
        ] {
            let dt = from_nanos(nanos).unwrap();
            let json = serde_json::to_string(&Wrap(dt)).unwrap();
            let back: Wrap = serde_json::from_str(&json).unwrap();
            assert_eq!(
                back.0,
                from_nanos(nanos).unwrap(),
                "instant changed for {nanos}"
            );
            // And the emitted form must itself parse back to the same instant.
            let s = json.trim_matches('"');
            assert_eq!(parse_rfc3339(s).unwrap(), from_nanos(nanos).unwrap());
        }
    }

    // Negative (pre-1970) sub-second timestamps must decode identically on both
    // backends. Regression guard for the chrono `div_euclid`/`rem_euclid` split.
    #[test]
    fn from_nanos_handles_negative_sub_second() {
        let cases: &[(i128, &str)] = &[
            (-1, "1969-12-31T23:59:59.999999999Z"),
            (-1_500_000_000, "1969-12-31T23:59:58.500000000Z"),
            (-1_000_000_000, "1969-12-31T23:59:59Z"),
        ];
        for (nanos, expected) in cases {
            let dt = from_nanos(*nanos).unwrap_or_else(|e| panic!("from_nanos({nanos}): {e}"));
            assert_eq!(dt, parse_rfc3339(expected).unwrap(), "mismatch for {nanos}");
        }
    }

    // The `rfc3339::option` adapter: exercise Some, explicit null, and an absent field.
    #[test]
    fn rfc3339_option_serde() {
        let some = WrapOpt(Some(from_nanos(FIXED_NANOS).unwrap()));
        let json = serde_json::to_string(&some).unwrap();
        assert_eq!(json, format!("\"{FIXED_RFC3339}\""));
        let back: WrapOpt = serde_json::from_str(&json).unwrap();
        assert_eq!(back.0, Some(from_nanos(FIXED_NANOS).unwrap()));

        let null: WrapOpt = serde_json::from_str("null").unwrap();
        assert_eq!(null.0, None);

        // Absent field with #[serde(default)] decodes to None.
        #[derive(Deserialize)]
        struct Outer {
            #[serde(default, with = "rfc3339::option")]
            ts: Option<DateTime>,
        }
        let outer: Outer = serde_json::from_str("{}").unwrap();
        assert_eq!(outer.ts, None);
    }

    // A non-UTC offset input must be normalized to the same instant as its UTC form.
    #[test]
    fn parse_rfc3339_normalizes_offset() {
        // 03:04:05+02:00 == 01:04:05Z
        let with_offset = parse_rfc3339("2023-01-02T03:04:05+02:00").unwrap();
        let utc = parse_rfc3339("2023-01-02T01:04:05Z").unwrap();
        assert_eq!(with_offset, utc);
    }

    // Compile-time proof of the backend selection: `time` by default, `chrono`
    // when the feature is enabled.
    #[cfg(not(feature = "chrono"))]
    #[test]
    fn datetime_is_time_by_default() {
        fn assert_offset_date_time(_: time::OffsetDateTime) {}
        assert_offset_date_time(now());
    }

    #[cfg(feature = "chrono")]
    #[test]
    fn datetime_is_chrono_when_enabled() {
        fn assert_chrono(_: chrono::DateTime<chrono::Utc>) {}
        assert_chrono(now());
    }
}
