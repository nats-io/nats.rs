#![allow(missing_docs)]
#![allow(unused)]
// Copyright 2020 The NATS Authors
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

//! Experimental Jetstream support enabled via the `jetstream` feature.

use std::{
    collections::VecDeque,
    convert::TryFrom,
    fmt::Debug,
    io::{self, Error, ErrorKind},
    time::UNIX_EPOCH,
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{jetstream_types::*, Connection as NatsClient};

// ApiResponse is a standard response from the JetStream JSON Api
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum ApiResponse<T> {
    Ok(T),
    Err { r#type: String, error: ApiError },
}

// ApiError is included in all Api responses if there was an error.
#[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize, Clone)]
struct ApiError {
    code: usize,
    description: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
struct PagedRequest {
    offset: i64,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct PagedResponse<T> {
    pub r#type: String,

    #[serde(alias = "streams", alias = "consumers")]
    pub items: Option<VecDeque<T>>,

    // related to paging
    pub total: usize,
    pub offset: usize,
    pub limit: usize,
}

#[derive(Debug)]
pub struct PagedIterator<'a, T> {
    manager: &'a Manager,
    subject: String,
    offset: i64,
    items: VecDeque<T>,
    done: bool,
}

impl<'a, T> std::iter::FusedIterator for PagedIterator<'a, T> where
    T: DeserializeOwned + Debug
{
}

impl<'a, T> Iterator for PagedIterator<'a, T>
where
    T: DeserializeOwned + Debug,
{
    type Item = io::Result<T>;

    fn next(&mut self) -> Option<io::Result<T>> {
        if self.done {
            return None;
        }
        if !self.items.is_empty() {
            return Some(Ok(self.items.pop_front().unwrap()));
        }
        let req = serde_json::ser::to_vec(&PagedRequest {
            offset: self.offset,
        })
        .unwrap();

        let res: io::Result<PagedResponse<T>> =
            self.manager.request(&self.subject, &req);

        if res.is_err() {
            self.done = true;
            return Some(Err(res.unwrap_err()));
        }

        let mut page = res.unwrap();

        if page.items.is_none() {
            self.done = true;
            return None;
        }

        let items = page.items.take().unwrap();

        self.offset += i64::try_from(items.len()).unwrap();
        self.items = items;

        if self.items.is_empty() {
            self.done = true;
            None
        } else {
            Some(Ok(self.items.pop_front().unwrap()))
        }
    }
}

/// Jetstream management functionality
#[derive(Debug)]
pub struct Manager {
    pub nc: NatsClient,
}

impl Manager {
    /// Create a stream.
    pub fn add_stream<S>(&self, stream_config: S) -> io::Result<StreamInfo>
    where
        StreamConfig: From<S>,
    {
        let cfg: StreamConfig = stream_config.into();
        if cfg.name.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "the stream name must not be empty",
            ));
        }
        let subject: String = format!("$JS.API.STREAM.CREATE.{}", cfg.name);
        let req = serde_json::ser::to_vec(&cfg)?;
        self.request(&subject, &req)
    }

    /// Update a stream.
    pub fn update_stream(
        &self,
        stream_config: StreamConfig,
    ) -> io::Result<StreamInfo> {
        self.add_stream(stream_config)
    }

    /// Query all stream names.
    pub fn stream_names<'a>(&'a self) -> PagedIterator<'a, String> {
        PagedIterator {
            subject: "$JS.API.STREAM.NAMES".into(),
            manager: self,
            offset: 0,
            items: Default::default(),
            done: false,
        }
    }

    /// List all stream names.
    pub fn list_streams<'a>(&'a self) -> PagedIterator<'a, StreamInfo> {
        PagedIterator {
            subject: "$JS.API.STREAM.LIST".into(),
            manager: self,
            offset: 0,
            items: Default::default(),
            done: false,
        }
    }

    /// Query stream information.
    pub fn stream_info<S: AsRef<str>>(
        &self,
        stream: S,
    ) -> io::Result<StreamInfo> {
        let stream: &str = stream.as_ref();
        if stream.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "the stream name must not be empty",
            ));
        }
        let subject: String = format!("$JS.API.STREAM.INFO.{}", stream);
        self.request(&subject, b"")
    }

    /// Purge stream messages.
    pub fn purge_stream<S: AsRef<str>>(
        &self,
        stream: S,
    ) -> io::Result<PurgeResponse> {
        let stream: &str = stream.as_ref();
        if stream.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "the stream name must not be empty",
            ));
        }
        let subject = format!("$JS.API.STREAM.PURGE.{}", stream);
        self.request(&subject, b"")
    }

    /// Delete message in a stream.
    pub fn delete_message<S: AsRef<str>>(
        &self,
        stream: S,
        sequence_number: u64,
    ) -> io::Result<()> {
        let stream: &str = stream.as_ref();
        if stream.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "the stream name must not be empty",
            ));
        }
        let subject = format!("$JS.API.STREAM.MSG.DELETE.{}", stream);
        let req = format!("{{ seq: {} }}", sequence_number);
        self.request(&subject, req.as_bytes())
    }

    /// Delete stream.
    pub fn delete_stream<S: AsRef<str>>(&self, stream: S) -> io::Result<bool> {
        let stream: &str = stream.as_ref();
        if stream.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "the stream name must not be empty",
            ));
        }

        #[derive(Deserialize)]
        struct DeleteResponse {
            success: bool,
        }

        let subject = format!("$JS.API.STREAM.DELETE.{}", stream);
        self.request::<DeleteResponse>(&subject, b"")
            .map(|dr| dr.success)
    }

    /// Create a consumer.
    pub fn add_consumer<S, C>(
        &self,
        stream: S,
        cfg: C,
    ) -> io::Result<ConsumerInfo>
    where
        S: AsRef<str>,
        ConsumerConfig: From<C>,
    {
        let mut config = ConsumerConfig::from(cfg);
        let stream = stream.as_ref();
        if stream.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "the stream name must not be empty",
            ));
        }

        let subject = if let Some(durable_name) = &config.durable_name {
            if durable_name.is_empty() {
                config.durable_name = None;
                format!("$JS.API.CONSUMER.CREATE.{}", stream)
            } else {
                format!(
                    "$JS.API.CONSUMER.DURABLE.CREATE.{}.{}",
                    stream, durable_name
                )
            }
        } else {
            format!("$JS.API.CONSUMER.CREATE.{}", stream)
        };

        let req = CreateConsumerRequest {
            stream_name: stream.into(),
            config,
        };

        let ser_req = serde_json::ser::to_vec(&req)?;

        self.request(&subject, &ser_req)
    }

    /// Delete a consumer.
    pub fn delete_consumer<S, C>(
        &self,
        stream: S,
        consumer: C,
    ) -> io::Result<bool>
    where
        S: AsRef<str>,
        C: AsRef<str>,
    {
        let stream = stream.as_ref();
        if stream.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "the stream name must not be empty",
            ));
        }
        let consumer = consumer.as_ref();
        if consumer.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "the consumer name must not be empty",
            ));
        }

        #[derive(Deserialize)]
        struct DeleteResponse {
            success: bool,
        }

        let subject =
            format!("$JS.API.CONSUMER.DELETE.{}.{}", stream, consumer);

        self.request::<DeleteResponse>(&subject, b"")
            .map(|dr| dr.success)
    }

    /// Query consumer information.
    pub fn consumer_info<S, C>(
        &self,
        stream: S,
        consumer: C,
    ) -> io::Result<ConsumerInfo>
    where
        S: AsRef<str>,
        C: AsRef<str>,
    {
        let stream: &str = stream.as_ref();
        if stream.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "the stream name must not be empty",
            ));
        }
        let consumer: &str = consumer.as_ref();
        let subject: String =
            format!("$JS.API.CONSUMER.INFO.{}.{}", stream, consumer);
        self.request(&subject, b"")
    }

    /// List consumers for a stream.
    pub fn list_consumers<'a, S>(
        &'a self,
        stream: S,
    ) -> io::Result<PagedIterator<'a, ConsumerInfo>>
    where
        S: AsRef<str>,
    {
        let stream: &str = stream.as_ref();
        if stream.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "the stream name must not be empty",
            ));
        }
        let subject: String = format!("$JS.API.CONSUMER.LIST.{}", stream);

        Ok(PagedIterator {
            subject,
            manager: self,
            offset: 0,
            items: Default::default(),
            done: false,
        })
    }

    /// Query account information.
    pub fn account_info(&self) -> io::Result<AccountInfo> {
        self.request("$JS.API.INFO", b"")
    }

    fn request<Res>(&self, subject: &str, req: &[u8]) -> io::Result<Res>
    where
        Res: DeserializeOwned,
    {
        let res_msg = self.nc.request(subject, req)?;
        println!("got response: {:?}", std::str::from_utf8(&res_msg.data));
        let res: ApiResponse<Res> = serde_json::de::from_slice(&res_msg.data)?;
        match res {
            ApiResponse::Ok(stream_info) => Ok(stream_info),
            ApiResponse::Err { error, .. } => {
                if let Some(desc) = error.description {
                    Err(Error::new(ErrorKind::Other, desc))
                } else {
                    Err(Error::new(ErrorKind::Other, "unknown"))
                }
            }
        }
    }
}

/// Jetstream reliable consumption functionality
pub struct Consumer {
    nc: NatsClient,
}

impl Consumer {
    /// Publishing messages to JetStream.
    pub fn publish(
        &self,
        subject: &str,
        data: &[u8],
        opts: Option<PubOpts>,
    ) -> io::Result<PubAck> {
        todo!()
    }

    /// Publishing messages to JetStream.
    pub fn publish_msg(
        &self,
        msg: Msg,
        opts: Option<PubOpts>,
    ) -> io::Result<PubAck> {
        todo!()
    }

    /// Subscribing to messages in JetStream.
    pub fn subscribe(
        &self,
        subj: String,
        cb: MsgHandler,
        opts: Option<SubOpts>,
    ) -> io::Result<Subscription> {
        todo!()
    }

    /// Subscribing to messages in JetStream.
    pub fn subscribe_sync(
        &self,
        subj: String,
        opts: Option<SubOpts>,
    ) -> io::Result<Subscription> {
        todo!()
    }

    /// Channel versions.
    pub fn chan_subscribe(
        &self,
        subj: String,
        ch: Chan<Msg>,
        opts: Option<SubOpts>,
    ) -> io::Result<Subscription> {
        todo!()
    }

    /// QueueSubscribe.
    pub fn queue_subscribe(
        &self,
        subj: String,
        queue: String,
        cb: MsgHandler,
        opts: Option<SubOpts>,
    ) -> io::Result<Subscription> {
        todo!()
    }
}

///
#[derive(Debug, Default, Clone)]
pub struct Subscription {
    consumer: String,
    stream: String,
    deliver: String,
    pull: i64,
    durable: bool,
    attached: bool,
}

///
#[derive(Debug, Default, Clone, Copy)]
pub struct Msg;

///
#[derive(Debug, Default, Clone, Copy)]
pub struct MsgHandler;

///
#[derive(Debug, Default, Clone, Copy)]
pub struct Context;

///
pub struct Chan<A>(A);

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    #[ignore]
    fn local_round_trip() -> io::Result<()> {
        let nc = crate::connect("localhost:4222").unwrap();
        let manager = Manager { nc };

        manager.add_stream(StreamConfig {
            name: "test1".to_string(),
            retention: RetentionPolicy::WorkQueue,
            ..Default::default()
        })?;

        manager.add_stream("test2")?;
        manager.stream_info("test2")?;
        manager.add_consumer("test2", "consumer1")?;
        manager.consumer_info("test2", "consumer1")?;

        let streams: io::Result<Vec<StreamInfo>> =
            manager.list_streams().collect();

        for stream in streams? {
            let consumers: io::Result<Vec<ConsumerInfo>> =
                manager.list_consumers(&stream.config.name)?.collect();

            for consumer in consumers? {
                manager.delete_consumer(&stream.config.name, &consumer.name)?;
            }

            manager.purge_stream(&stream.config.name)?;
            manager.delete_stream(&stream.config.name)?;
        }
        dbg!(manager.account_info());
        Ok(())
    }
}
