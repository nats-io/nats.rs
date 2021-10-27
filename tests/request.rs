use nats::MessageError;
use async_std::task;
use std::str::from_utf8;
use std::sync::mpsc;

#[test]
fn no_responders() -> Result<(), String> {
    let nc = nats::Options::new().connect("demo.nats.io").unwrap();
    let res = nc.request("noresponders", "message");
    match res {
        Err(e) => {
            if e.to_string() == String::from("no responders") {
                Ok(())
            } else {
                Err(String::from(format!("should be no responders, but is {}", e)))
            }
        },
        Ok(_) => Err(String::from("should be no responders")),
    }
}

#[test]
fn no_responders_async() -> Result<(), String> {
    smol::block_on(async {
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
    })

}
