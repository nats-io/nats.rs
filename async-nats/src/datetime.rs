// Copyright 2020-2026
// TODO

#[cfg(all(
    any(feature = "jetstream", feature = "service"),
    not(feature = "time"),
    not(feature = "chrono")
))]
compile_error!(
    "Either 'time' or 'chrono' feature must be enabled when 'jetstream' or 'service' is active."
);

#[cfg(feature = "chrono")]
pub type DateTime = chrono::DateTime<chrono::Utc>;

#[cfg(all(feature = "time", not(feature = "chrono")))]
pub type DateTime = time::OffsetDateTime;

#[cfg(feature = "chrono")]
pub fn parse_rfc3339(s: &str) -> Result<DateTime, crate::Error> {
    chrono::DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .map_err(|e| e.into())
}

#[cfg(all(feature = "time", not(feature = "chrono")))]
pub fn parse_rfc3339(s: &str) -> Result<DateTime, crate::Error> {
    time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
        .map_err(|e| e.into())
}

#[cfg(feature = "chrono")]
pub fn from_nanos(nanos: i128) -> Result<DateTime, crate::Error> {
    let nanos_i64: i64 = nanos.try_into().map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "nanoseconds value too large for chrono",
        )
    })?;
    let secs = nanos_i64 / 1_000_000_000;
    let nsecs = (nanos_i64 % 1_000_000_000) as u32;
    chrono::DateTime::from_timestamp(secs, nsecs).ok_or_else(|| {
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "invalid timestamp",
        )) as crate::Error
    })
}

#[cfg(all(feature = "time", not(feature = "chrono")))]
pub fn from_nanos(nanos: i128) -> Result<DateTime, crate::Error> {
    time::OffsetDateTime::from_unix_timestamp_nanos(nanos).map_err(|e| e.into())
}

#[cfg(feature = "chrono")]
pub fn now() -> DateTime {
    chrono::Utc::now()
}

#[cfg(all(feature = "time", not(feature = "chrono")))]
pub fn now() -> DateTime {
    time::OffsetDateTime::now_utc()
}

#[cfg(feature = "chrono")]
pub mod rfc3339 {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    pub fn serialize<S>(
        dt: &chrono::DateTime<chrono::Utc>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        Serialize::serialize(dt, serializer)
    }
    pub fn deserialize<'de, D>(deserializer: D) -> Result<chrono::DateTime<chrono::Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Deserialize::deserialize(deserializer)
    }

    pub mod option {
        use serde::{Deserialize, Deserializer, Serialize, Serializer};
        pub fn serialize<S>(
            dt: &Option<chrono::DateTime<chrono::Utc>>,
            serializer: S,
        ) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            Serialize::serialize(dt, serializer)
        }
        pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<chrono::DateTime<chrono::Utc>>, D::Error>
        where
            D: Deserializer<'de>,
        {
            Deserialize::deserialize(deserializer)
        }
    }
}

#[cfg(all(feature = "time", not(feature = "chrono")))]
pub use time::serde::rfc3339;

#[cfg(feature = "chrono")]
pub fn add_std_duration(dt: DateTime, duration: std::time::Duration) -> DateTime {
    let chrono_duration = chrono::Duration::from_std(duration).unwrap();
    dt + chrono_duration
}

#[cfg(all(feature = "time", not(feature = "chrono")))]
pub fn add_std_duration(dt: DateTime, duration: std::time::Duration) -> DateTime {
    let time_duration = time::Duration::try_from(duration).unwrap();
    dt + time_duration
}
