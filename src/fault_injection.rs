#![allow(clippy::float_arithmetic)]

use std::io::{self, Error, ErrorKind};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

/// This function is useful for inducing random jitter into our operations that
/// trigger cross-thread communication, shaking out more possible interleavings
/// quickly. It gets fully eliminated by the compiler in non-test code.
pub fn inject_delay() {
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

    if fastrand::i32(..10) == 0 {
        let duration = fastrand::u64(..50);

        #[allow(clippy::cast_possible_truncation)]
        #[allow(clippy::cast_sign_loss)]
        thread::sleep(Duration::from_millis(duration));
    }

    if fastrand::i32(..2) == 0 {
        thread::yield_now();
    }
}

/// This allows our IO error handling code to be tested by
/// injecting failures sometimes.
pub fn inject_io_failure() -> io::Result<()> {
    if fastrand::i32(..100) == 0 {
        Err(Error::new(ErrorKind::Other, "injected fault"))
    } else {
        Ok(())
    }
}
