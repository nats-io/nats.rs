use std::{
    fmt,
    ops::Deref,
    ptr::write_volatile,
    sync::atomic::{compiler_fence, Ordering::SeqCst},
};

use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};

/// Provides a vector that will scramble allocations as
/// they are discarded when the vector grows.
///
/// Allows users to scamble the data whenever they want.
///
/// Also scrambles data on Drop.
///
/// Uses the basic idea (write_volatile + compiler_fence)
/// from @bascule's zeroize crate but overwrites data with
/// random bytes instead of zeros.
pub(crate) struct SecureVec {
    inner: Vec<u8>,
}

impl Drop for SecureVec {
    fn drop(&mut self) {
        self.scramble();
    }
}

impl SecureVec {
    pub(crate) fn with_capacity(sz: usize) -> SecureVec {
        SecureVec {
            inner: Vec::with_capacity(sz),
        }
    }

    pub(crate) fn push(&mut self, item: u8) {
        if self.inner.len() == self.inner.capacity() {
            let cap = std::cmp::max(16, self.inner.capacity());
            let mut next = Vec::with_capacity(cap * 2);

            // copy toxic waste to next destination
            next.extend_from_slice(&self.inner);

            // replace old home of toxic waste with random data
            self.scramble();

            self.inner = next;
        }
        self.inner.push(item);
    }

    pub(crate) fn scramble(&mut self) {
        let mut rng = thread_rng();
        for byte in &mut self.inner {
            #[allow(unsafe_code)]
            unsafe {
                write_volatile(byte, rng.gen());
            }
        }
        compiler_fence(SeqCst);
    }
}

impl Deref for SecureVec {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.inner
    }
}

/// Provides a `String` that will scramble allocations as
/// they are discarded when the `String` grows.
///
/// Scrambles data on Drop.
///
/// Uses the basic idea (write_volatile + compiler_fence)
/// from @bascule's zeroize crate but overwrites data with
/// random bytes instead of zeros.
#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct SecureString {
    inner: String,
}

impl fmt::Debug for SecureString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_map().entry(&"secure_data", &"***********").finish()
    }
}

impl Drop for SecureString {
    fn drop(&mut self) {
        self.scramble();
    }
}

impl From<String> for SecureString {
    fn from(inner: String) -> SecureString {
        SecureString { inner }
    }
}

impl SecureString {
    pub(crate) fn scramble(&mut self) {
        let mut rng = thread_rng();

        #[allow(unsafe_code)]
        unsafe {
            for byte in self.inner.as_bytes_mut() {
                write_volatile(byte, rng.gen());
            }
        }
        compiler_fence(SeqCst);
    }
}

impl Deref for SecureString {
    type Target = str;

    fn deref(&self) -> &str {
        &self.inner
    }
}
