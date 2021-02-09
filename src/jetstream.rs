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

//! Experimental `JetStream` support enabled via the `jetstream` feature.

use std::{
    collections::VecDeque,
    convert::TryFrom,
    fmt::Debug,
    io::{self, Error, ErrorKind},
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{jetstream_types::*, Connection as NatsClient, Message};

/// `ApiResponse` is a standard response from the `JetStream` JSON Api
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum ApiResponse<T> {
    Ok(T),
    Err { r#type: String, error: ApiError },
}

/// `ApiError` is included in all Api responses if there was an error.
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
struct PagedResponse<T> {
    pub r#type: String,

    #[serde(alias = "streams", alias = "consumers")]
    pub items: Option<VecDeque<T>>,

    // related to paging
    pub total: usize,
    pub offset: usize,
    pub limit: usize,
}

/// An iterator over paged `JetStream` API operations.
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

        let mut page = match res {
            Err(e) => {
                self.done = true;
                return Some(Err(e));
            }
            Ok(page) => page,
        };

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

/// `JetStream` management functionality
#[derive(Debug)]
pub struct Manager {
    /// The underlying NATS client
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
    pub fn stream_names(&self) -> PagedIterator<'_, String> {
        PagedIterator {
            subject: "$JS.API.STREAM.NAMES".into(),
            manager: self,
            offset: 0,
            items: Default::default(),
            done: false,
        }
    }

    /// List all stream names.
    pub fn list_streams(&self) -> PagedIterator<'_, StreamInfo> {
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
    ) -> io::Result<bool> {
        let stream: &str = stream.as_ref();
        if stream.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "the stream name must not be empty",
            ));
        }

        #[derive(Serialize)]
        struct DeleteRequest {
            seq: u64,
        }

        let req = serde_json::ser::to_vec(&DeleteRequest {
            seq: sequence_number,
        })
        .unwrap();

        let subject = format!("$JS.API.STREAM.MSG.DELETE.{}", stream);

        self.request::<DeleteResponse>(&subject, &req)
            .map(|dr| dr.success)
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
    pub fn list_consumers<S>(
        &self,
        stream: S,
    ) -> io::Result<PagedIterator<'_, ConsumerInfo>>
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

/// `JetStream` reliable consumption functionality
pub struct Consumer {
    /// The underlying NATS client
    pub nc: NatsClient,

    /// The stream that this `Consumer` is interested in
    pub stream: String,

    /// The backing configuration for this `Consumer`
    pub cfg: ConsumerConfig,
}

impl Consumer {
    /// Instantiate a new `Consumer`
    pub fn new<S, C>(nc: NatsClient, stream: S, cfg: C) -> Consumer
    where
        S: AsRef<str>,
        ConsumerConfig: From<C>,
    {
        let stream = stream.as_ref().to_string();
        Consumer {
            nc,
            stream,
            cfg: cfg.into(),
        }
    }

    /// Process a single message for a pull-based consumer.
    pub fn process_batch<R, F: FnMut(&Message) -> R>(
        &self,
        batch_size: usize,
        mut f: F,
    ) -> io::Result<Vec<R>> {
        if self.cfg.durable_name.is_none() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "process and process_batch are only usable from \
                Pull-based Consumers with a durable_name set",
            ));
        }

        let subject = format!(
            "$JS.API.CONSUMER.MSG.NEXT.{}.{}",
            self.stream,
            self.cfg.durable_name.as_ref().unwrap()
        );

        let responses = if let Some(deliver_subject) = &self.cfg.deliver_subject
        {
            self.nc.subscribe(&deliver_subject)?
        } else {
            self.nc.request_multi(&subject, batch_size.to_string())?
        };
        let mut rets = Vec::with_capacity(batch_size);
        let mut last = None;

        let mut received = 0;
        while let Ok(msg) =
            responses.next_timeout(std::time::Duration::from_millis(100))
        {
            let ret = f(&msg);

            if self.cfg.ack_policy == AckPolicy::Explicit {
                msg.respond(b"")?;
            }

            rets.push(ret);
            last = Some(msg);
            received += 1;
            if received == batch_size {
                break;
            }
        }

        if let Some(msg) = last {
            if self.cfg.ack_policy == AckPolicy::All {
                msg.respond(b"")?
            }
        }

        Ok(rets)
    }

    /// Process a single message for a pull-based consumer.
    pub fn process<R, F: Fn(&Message) -> R>(&self, f: F) -> io::Result<R> {
        if self.cfg.durable_name.is_none() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "process and process_batch are only usable from \
                Pull-based Consumers with a durable_name set",
            ));
        }

        let subject = format!(
            "$JS.API.CONSUMER.MSG.NEXT.{}.{}",
            self.stream,
            self.cfg.durable_name.as_ref().unwrap()
        );

        let next = if let Some(deliver_subject) = &self.cfg.deliver_subject {
            self.nc.subscribe(&deliver_subject)?.next().unwrap()
        } else {
            self.nc.request(&subject, b"")?
        };
        let ret = f(&next);
        next.respond(b"")?;
        Ok(ret)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    #[ignore]
    fn local_round_trip() -> io::Result<()> {
        let nc = crate::connect("localhost:4222").unwrap();
        let manager = Manager { nc };

        let _ = manager.delete_stream("test1");
        let _ = manager.delete_stream("test2");

        manager.add_stream(StreamConfig {
            name: "test1".to_string(),
            retention: RetentionPolicy::WorkQueue,
            ..Default::default()
        })?;

        manager.add_stream("test2")?;
        manager.stream_info("test2")?;
        manager.add_consumer("test2", "consumer1")?;

        let consumer2_cfg = ConsumerConfig {
            durable_name: Some("consumer2".to_string()),
            ack_policy: AckPolicy::All,
            deliver_subject: Some("consumer2_ds".to_string()),
            ..Default::default()
        };
        manager.add_consumer("test2", &consumer2_cfg)?;
        manager.consumer_info("test2", "consumer1")?;

        for i in 1..=1000 {
            manager.nc.publish("test2", format!("{}", i))?;
        }

        assert_eq!(manager.stream_info("test2")?.state.messages, 1000);

        let consumer1 = Consumer::new(manager.nc.clone(), "test2", "consumer1");

        for _ in 1..=1000 {
            consumer1.process(|_msg| {})?;
        }

        let consumer2 =
            Consumer::new(manager.nc.clone(), "test2", consumer2_cfg);

        let mut count = 0;
        consumer2.process_batch(1000, |_msg| {
            count += 1;
        })?;
        assert_eq!(count, 1000);

        // sequence numbers start with 1
        for i in 1..=500 {
            manager.delete_message("test2", i)?;
        }

        assert_eq!(manager.stream_info("test2")?.state.messages, 500);

        let _ = dbg!(manager.account_info());

        // cleanup
        let streams: io::Result<Vec<StreamInfo>> =
            manager.list_streams().collect();

        for stream in streams? {
            let consumers: io::Result<Vec<ConsumerInfo>> =
                manager.list_consumers(&stream.config.name)?.collect();

            for consumer in consumers? {
                manager.delete_consumer(&stream.config.name, &consumer.name)?;
            }

            manager.purge_stream(&stream.config.name)?;

            assert_eq!(
                manager.stream_info(&stream.config.name)?.state.messages,
                0
            );

            manager.delete_stream(&stream.config.name)?;
        }

        Ok(())
    }
}
