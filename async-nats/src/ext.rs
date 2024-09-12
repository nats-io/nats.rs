use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::Stream;
use serde::de::DeserializeOwned;

pub trait SubscribeExt<M>: Stream<Item = M>
where
    M: MessageTrait,
{
    fn for_type<T>(self) -> TypedStream<Self, T>
    where
        Self: Sized,
        T: DeserializeOwned,
    {
        TypedStream::new(self)
    }
}

impl<S, M> SubscribeExt<M> for S
where
    S: Stream<Item = M>,
    M: MessageTrait,
{
}

pin_project_lite::pin_project! {
    pub struct TypedStream<S, T> {
        #[pin]
        stream: S,
        _phantom: std::marker::PhantomData<T>,
    }
}

impl<S, T> TypedStream<S, T> {
    fn new(stream: S) -> Self {
        Self {
            stream,
            _phantom: std::marker::PhantomData,
        }
    }
}

pub trait MessageTrait {
    // fn payload(&self) -> Bytes;
    // fn subject(&self) -> Subject;
    // fn reply(&self) -> Option<Subject>;
    // fn headers(&self) -> Option<HeaderMap>;
    // fn status(&self) -> Option<StatusCode>;
    // fn description(&self) -> Option<String>;
    // fn length(&self) -> usize;
    fn payload(&self) -> &[u8];
}

impl MessageTrait for crate::Message {
    fn payload(&self) -> &[u8] {
        self.payload.as_ref()
    }
}

impl MessageTrait for crate::PublishMessage {
    fn payload(&self) -> &[u8] {
        self.payload.as_ref()
    }
}

impl MessageTrait for crate::jetstream::message::Message {
    fn payload(&self) -> &[u8] {
        self.payload.as_ref()
    }
}

impl<S, T, M> Stream for TypedStream<S, T>
where
    S: Stream<Item = M>,
    T: DeserializeOwned,
    M: MessageTrait,
{
    type Item = serde_json::Result<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        match this.stream.poll_next(cx) {
            Poll::Ready(message) => match message {
                Some(message) => {
                    let message = message.payload();
                    Poll::Ready(Some(serde_json::from_slice(&message)))
                }
                None => Poll::Ready(None),
            },
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod test {
    use futures::StreamExt;
    use futures::TryStreamExt;
    use serde::Serialize;

    use super::SubscribeExt;
    use crate::PublishMessage;

    #[tokio::test]
    async fn for_type() {
        use futures::stream;
        use serde::Deserialize;

        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct Test {
            a: i32,
            b: String,
        }

        struct OtherTest {
            data: (i32, String),
        }

        // Prepare some messages
        let messages = vec![
            PublishMessage {
                subject: "test".into(),
                payload: serde_json::to_vec(&Test {
                    a: 1,
                    b: "a".to_string(),
                })
                .unwrap()
                .into(),
                reply: None,
                headers: Default::default(),
            },
            PublishMessage {
                subject: "test".into(),
                payload: serde_json::to_vec(&Test {
                    a: 2,
                    b: "b".to_string(),
                })
                .unwrap()
                .into(),
                reply: None,
                headers: Default::default(),
            },
        ];

        // Simulate a stream of messages
        let stream = stream::iter(messages);

        // first deserialize into a concrete type
        let stream = stream
            .for_type::<Test>()
            // and then transform into another type
            .and_then(|item| async move {
                Ok(OtherTest {
                    data: (item.a, item.b),
                })
            });

        // Don't worry, that is just Rust bs about pinning data.
        let mut stream = Box::pin(stream);

        // see that it works.
        assert_eq!(stream.next().await.unwrap().unwrap().data.0, 1);
        assert_eq!(stream.next().await.unwrap().unwrap().data.0, 2);
    }
}
