use std::{
    fmt,
    ops::Deref,
    ptr::write_volatile,
    sync::atomic::{compiler_fence, Ordering::SeqCst},
};

/// A `Vec<u8>` that gets scrambled on drop.
/// Provides a vector that will scramble allocations as
/// they are discarded when the vector grows.
///
/// Allows users to scamble the data whenever they want.
///
/// Also scrambles data on Drop.
///
/// Uses the basic idea (`write_volatile` + `compiler_fence`)
/// from @bascule's zeroize crate but overwrites data with
/// random bytes instead of zeros.
#[derive(Clone, Default)]
pub(crate) struct SecureVec(Vec<u8>);

impl SecureVec {
    pub(crate) fn with_capacity(sz: usize) -> SecureVec {
        SecureVec(Vec::with_capacity(sz))
    }

    pub(crate) fn push(&mut self, item: u8) {
        if self.0.len() == self.0.capacity() {
            let cap = std::cmp::max(16, self.0.capacity());
            let mut next = Vec::with_capacity(cap * 2);

            // copy toxic waste to next destination
            next.extend_from_slice(&self.0);

            // replace old home of toxic waste with random data
            self.scramble();

            self.0 = next;
        }
        self.0.push(item);
    }

    pub(crate) fn scramble(&mut self) {
        for byte in &mut self.0 {
            #[allow(unsafe_code)]
            unsafe {
                write_volatile(byte, fastrand::u8(..));
            }
        }
        compiler_fence(SeqCst);
    }
}

impl Drop for SecureVec {
    fn drop(&mut self) {
        self.scramble();
    }
}

impl From<Vec<u8>> for SecureVec {
    fn from(inner: Vec<u8>) -> SecureVec {
        SecureVec(inner)
    }
}

impl Deref for SecureVec {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.0
    }
}

/// A `String` that gets scrambled on drop.
///
/// Uses the basic idea (`write_volatile` + `compiler_fence`)
/// from @bascule's zeroize crate but overwrites data with
/// random bytes instead of zeros.
///
/// Public + hidden for testing purposes.
#[derive(Clone, Default)]
#[doc(hidden)]
pub struct SecureString(String);

impl SecureString {
    pub(crate) fn scramble(&mut self) {
        #[allow(unsafe_code)]
        unsafe {
            for byte in self.0.as_bytes_mut() {
                write_volatile(byte, fastrand::u8(..));
            }
        }
        compiler_fence(SeqCst);
    }
}

impl Drop for SecureString {
    fn drop(&mut self) {
        self.scramble();
    }
}

impl fmt::Debug for SecureString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_map().entry(&"secure_data", &"***********").finish()
    }
}

impl From<String> for SecureString {
    fn from(inner: String) -> SecureString {
        SecureString(inner)
    }
}

impl Deref for SecureString {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}
