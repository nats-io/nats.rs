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

//! Internal ID generation for inbox subjects and object store nonces.
//!
//! By default, uses NUID for high-performance, cryptographically strong IDs.
//! When the `nuid` feature is disabled, falls back to rand-based alphanumeric generation.

/// Generate a unique ID string.
///
/// With the `nuid` feature (default): Uses NUID for high-performance,
/// cryptographically strong, collision-resistant IDs.
///
/// Without `nuid` feature: Uses rand to generate 22-character alphanumeric IDs,
/// which is lighter on dependencies but slightly less performant.
#[cfg(feature = "nuid")]
#[inline]
pub(crate) fn next() -> String {
    nuid::next().to_string()
}

/// Generate a unique ID string using rand-based alphanumeric generation.
///
/// This is used when the `nuid` feature is disabled. Generates a 22-character
/// alphanumeric string using rand's thread_rng, matching NUID's output length.
#[cfg(not(feature = "nuid"))]
#[inline]
pub(crate) fn next() -> String {
    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};

    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(22)
        .map(char::from)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_next_generates_non_empty() {
        let id = next();
        assert!(!id.is_empty());
    }

    #[test]
    fn test_next_generates_unique() {
        let id1 = next();
        let id2 = next();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_next_reasonable_length() {
        let id = next();
        // NUID generates 22 characters, our rand fallback does too
        assert!(id.len() >= 20 && id.len() <= 30);
    }

    #[test]
    fn test_next_alphanumeric() {
        let id = next();
        assert!(id.chars().all(|c| c.is_alphanumeric()));
    }
}
