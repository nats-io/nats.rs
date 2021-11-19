use smol::future::FutureExt;
use std::io;

mod nats_server;
pub use nats_server::*;

#[test]
fn drop_flushes() -> io::Result<()> {
    let s = nats_server::run_basic_server();

    let nc1 = nats::connect(&s.client_url())?;
    let nc2 = nats::connect(&s.client_url())?;

    let inbox = nc1.new_inbox();
    let sub = nc2.subscribe(&inbox)?;
    nc2.flush()?;

    nc1.publish(&inbox, b"hello")?;
    drop(nc1); // Dropping should flush the published message.

    assert_eq!(sub.next().unwrap().data, b"hello");

    Ok(())
}

#[test]
fn two_connections() -> io::Result<()> {
    let s = nats_server::run_basic_server();

    let nc1 = nats::connect(&s.client_url())?;
    let nc2 = nc1.clone();

    nc1.publish("foo", b"bar")?;
    nc2.publish("foo", b"bar")?;

    drop(nc1);
    nc2.publish("foo", b"bar")?;

    Ok(())
}

#[test]
fn async_subscription_drop() -> io::Result<()> {
    let s = nats_server::run_basic_server();

    smol::block_on(async {
        let nc = nats::asynk::connect(&s.client_url()).await?;

        let inbox = nc.new_inbox();

        // This makes sure the subscription is closed after being dropped. If it wasn't closed,
        // creating the 501st subscription would block forever due to the `blocking` crate's thread
        // pool being fully occupied.
        for _ in 0..600 {
            let sub = nc
                .subscribe(&inbox)
                .or(async {
                    smol::Timer::after(std::time::Duration::from_secs(2)).await;
                    Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "unable to create subscription",
                    ))
                })
                .await?;
            sub.next()
                .or(async {
                    smol::Timer::after(std::time::Duration::from_millis(1)).await;
                    None
                })
                .await;
        }

        Ok(())
    })
}
