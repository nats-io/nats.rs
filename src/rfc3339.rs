use serde::{Deserialize, Deserializer, Serializer};
use time::OffsetDateTime;

pub(crate) fn serialize<S>(date_time: &OffsetDateTime, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let s = date_time
        .format(&time::format_description::well_known::Rfc3339)
        .map_err(serde::ser::Error::custom)?;
    serializer.serialize_str(&s)
}

pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<OffsetDateTime, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    OffsetDateTime::parse(&s, &time::format_description::well_known::Rfc3339)
        .map_err(serde::de::Error::custom)
}

pub(crate) mod option {
    use serde::{Deserialize, Deserializer, Serializer};
    use time::OffsetDateTime;

    pub(crate) fn serialize<'a, S>(
        date_time: &'a Option<OffsetDateTime>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if let Some(date_time) = date_time {
            let s = date_time
                .format(&time::format_description::well_known::Rfc3339)
                .map_err(serde::ser::Error::custom)?;
            serializer.serialize_some(&s)
        } else {
            serializer.serialize_none()
        }
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Option<OffsetDateTime>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(Some(
            OffsetDateTime::parse(&s, &time::format_description::well_known::Rfc3339)
                .map_err(serde::de::Error::custom)?,
        ))
    }
}
