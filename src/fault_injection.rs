#![allow(clippy::float_arithmetic)]

use std::io::{self, Error, ErrorKind};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

use rand::{thread_rng, Rng};
use smol::Timer;

/// This function is useful for inducing random jitter into our operations that trigger
/// cross-thread communication, shaking out more possible interleavings quickly. It gets fully
/// eliminated by the compiler in non-test code.
pub async fn inject_delay() {
    use std::thread;
    use std::time::Duration;

    static GLOBAL_DELAYS: AtomicUsize = AtomicUsize::new(0);

    thread_local!(
        static LOCAL_DELAYS: std::cell::RefCell<usize> = std::cell::RefCell::new(0)
    );

    let global_delays = GLOBAL_DELAYS.fetch_add(1, Relaxed);
    let local_delays = LOCAL_DELAYS.with(|ld| {
        let mut ld = ld.borrow_mut();
        let old = *ld;
        *ld = std::cmp::max(global_delays + 1, *ld + 1);
        old
    });

    if global_delays == local_delays {
        // no other threads seem to be calling this, so we don't
        // gain anything by injecting delays.
        return;
    }

    if thread_rng().gen_ratio(1, 10) {
        let duration = thread_rng().gen_range(0, 50);

        #[allow(clippy::cast_possible_truncation)]
        #[allow(clippy::cast_sign_loss)]
        Timer::new(Duration::from_millis(duration)).await;
    }

    if thread_rng().gen_ratio(1, 2) {
        thread::yield_now();
    }
}

/// This allows our IO error handling code to be tested by
/// injecting failures sometimes.
pub fn inject_io_failure() -> io::Result<()> {
    if thread_rng().gen_ratio(1, 100) {
        Err(Error::new(ErrorKind::Other, "injected fault"))
    } else {
        Ok(())
    }
}
