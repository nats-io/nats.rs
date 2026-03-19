// #[cfg(not(any(feature = "aws-lc-rs", feature = "ring")))]
// compile_error!("Please enable the `aws-lc-rs` or `ring` feature");

// #[cfg(not(target_arch = "wasm32"))]
// use crypto_backend::digest::{Context, SHA256};

// #[cfg(not(target_arch = "wasm32"))]
// type Sha256Context = Context;

// #[cfg(target_arch = "wasm32")]
// type Sha256Context = sha2::Sha256::Hash;

use sha2::Digest;

#[allow(dead_code)]
pub(crate) struct Sha256(sha2::Sha256);

#[allow(dead_code)]
impl Sha256 {
    pub(crate) fn new() -> Self {
        Self(sha2::Sha256::new())
    }

    pub(crate) fn update(&mut self, chunk: &[u8]) {
        self.0.update(chunk);
    }

    pub(crate) fn finish(self) -> [u8; 32] {
        let digest = self.0.finalize();
        digest.try_into().expect("sha256 hash is 32 bytes")
    }
}
