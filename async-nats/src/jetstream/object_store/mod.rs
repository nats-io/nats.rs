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

const DEFAULT_CHUNK_SIZE: usize = 128 * 1024;
const NATS_ROLLUP: &str = "Nats-Rollup";
const ROLLUP_SUBJECT: &str = "sub";

lazy_static! {
    static ref BUCKET_NAME_RE: Regex = Regex::new(r#"\A[a-zA-Z0-9_-]+\z"#).unwrap();
    static ref OBJECT_NAME_RE: Regex = Regex::new(r#"\A[-/_=\.a-zA-Z0-9]+\z"#).unwrap();
}

fn is_valid_bucket_name(bucket_name: &str) -> bool {
    BUCKET_NAME_RE.is_match(bucket_name)
}

fn is_valid_object_name(object_name: &str) -> bool {
    if object_name.is_empty() || object_name.starts_with('.') || object_name.ends_with('.') {
        return false;
    }

    OBJECT_NAME_RE.is_match(object_name)
}

fn sanitize_object_name(object_name: &str) -> String {
    object_name.replace('.', "_").replace(' ', "_")
}

/// Configuration values for object store buckets.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Name of the storage bucket.
    pub bucket: String,
    /// A short description of the purpose of this storage bucket.
    pub description: Option<String>,
    /// Maximum age of any value in the bucket, expressed in nanoseconds
    pub max_age: Duration,
    /// The type of storage backend, `File` (default) and `Memory`
    pub storage: StorageType,
    /// How many replicas to keep for each value in a cluster, maximum 5.
    pub num_replicas: usize,
}
