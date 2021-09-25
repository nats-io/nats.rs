use nats::MessageError;
use async_std::task;
use std::str::from_utf8;
use std::sync::mpsc;

#[test]
fn no_responders() -> Result<(), String> {
    let nc = nats::Options::new().connect("demo.nats.io").unwrap();
    let res = nc.request("noresponders", "message").unwrap();
    match res.err {
        Some(MessageError::NoResponders) => Ok(()),
        None => Err(String::from("should be no responders error, got None")),
    }
}

#[async_std::test]
async fn no_responders_async() -> Result<(), String> {
    let nc = nats::asynk::Options::new().connect("demo.nats.io").await.unwrap();
    let anc = nc.clone();
    let (tx, rx) = mpsc::channel();
    let child = task::spawn(async move   {
        let sub = anc.subscribe("noresponders.>").await.unwrap();
        tx.send(true).unwrap();
        let msg =  sub.next().await.unwrap();
        anc.publish(msg.reply.unwrap().as_str(), "pong").await.unwrap();
    });
    rx.recv().unwrap();
    let req = nc.request("noresponders.asdf", "ping").await.unwrap();
    match req.err {
        Some(MessageError::NoResponders) => Err(String::from("found no responders error, should be ok")),
        None => Ok(()),
    }?;
    assert_eq!("pong", from_utf8(&req.data).unwrap());
    let _ = child.await;
    Ok(())
}
