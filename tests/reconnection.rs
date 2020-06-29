use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Barrier,
    },
    thread,
    time::Duration,
};

use nats_test_server::nats_test_server;

#[test]
#[ignore]
fn reconnect_test() {
    env_logger::init();

    let shutdown = Arc::new(AtomicBool::new(false));
    let restart = Arc::new(AtomicBool::new(false));
    let success = Arc::new(AtomicBool::new(false));

    // kill process if we take longer than 15 minutes to run the test
    thread::spawn({
        let success = success.clone();
        move || {
            thread::sleep(Duration::from_secs(15 * 60));
            if !success.load(Ordering::Acquire) {
                log::error!("killing process after 15 minutes");
                std::process::exit(1);
            }
        }
    });

    let barrier = Arc::new(Barrier::new(2));
    let server = thread::spawn({
        let barrier = barrier.clone();
        let shutdown = shutdown.clone();
        let hop_ports = false;
        let bugginess = 200;
        move || {
            nats_test_server(
                "localhost",
                22222,
                barrier,
                shutdown,
                restart,
                bugginess,
                hop_ports,
            )
        }
    });

    barrier.wait();

    let nc = loop {
        if let Ok(nc) = nats::Options::new()
            .max_reconnects(None)
            .connect("localhost:22222")
        {
            break Arc::new(nc);
        }
    };

    let tx = thread::spawn({
        let nc = nc.clone();
        let success = success.clone();
        let shutdown = shutdown.clone();
        move || {
            const EXPECTED_SUCCESSES: usize = 25;
            let mut received = 0;

            while received < EXPECTED_SUCCESSES && !shutdown.load(Ordering::Acquire) {
                if nc
                    .request_timeout(
                        "rust.tests.faulty_requests",
                        "Help me?",
                        std::time::Duration::from_millis(200),
                    )
                    .is_ok()
                {
                    received += 1;
                } else {
                    log::debug!("timed out before we received a response :(");
                }
            }

            if received == EXPECTED_SUCCESSES {
                success.store(true, Ordering::Release);
            }
        }
    });

    let subscriber = loop {
        if let Ok(subscriber) = nc.subscribe("rust.tests.faulty_requests") {
            break subscriber;
        }
    };

    while !success.load(Ordering::Acquire) {
        for msg in subscriber.timeout_iter(Duration::from_millis(10)) {
            let _unchecked = msg.respond("Anything for the story");
        }
    }

    shutdown.store(true, Ordering::Release);

    tx.join().unwrap();
    server.join().unwrap();
}
