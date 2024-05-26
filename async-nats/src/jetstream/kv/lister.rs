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

use std::{
    pin::Pin,
    task::{ready, Poll},
};

use futures::TryStreamExt;

use crate::jetstream::{
    context::{StreamNames, Streams, StreamsError},
    Context,
};

pub struct KeyValueNames {
    stream_names: StreamNames,
}

impl KeyValueNames {
    pub fn new(ctx: &Context) -> Self {
        let mut stream_names = ctx.stream_names();
        stream_names.subjects = Some("$KV.*.>".to_owned());

        Self { stream_names }
    }
}

impl futures::Stream for KeyValueNames {
    type Item = Result<String, StreamsError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Ready(loop {
            if let Some(name) = ready!(self.stream_names.try_poll_next_unpin(cx)?) {
                let name = name.strip_prefix("KV_").map(str::to_owned);

                if let Some(name) = name {
                    break Some(Ok(name));
                }
            } else {
                // The stream is done
                break None;
            }
        })
    }
}

pub struct KeyValueStores {
    streams: Streams,
}

impl KeyValueStores {
    pub fn new(ctx: &Context) -> Self {
        let mut streams = ctx.streams();
        streams.subjects = Some("$KV.*.>".to_owned());

        Self { streams }
    }
}

impl futures::Stream for KeyValueStores {
    type Item = Result<super::bucket::Status, StreamsError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Ready(loop {
            if let Some(info) = ready!(self.streams.try_poll_next_unpin(cx)?) {
                let bucket_name = info.config.name.strip_prefix("KV_").map(str::to_owned);

                if let Some(bucket) = bucket_name {
                    break Some(Ok(super::bucket::Status { info, bucket }));
                }
            } else {
                // The stream is done
                break None;
            }
        })
    }
}
