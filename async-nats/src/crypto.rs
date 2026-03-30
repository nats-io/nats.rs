#[cfg(not(target_arch = "wasm32"))]
mod crypto_backend {
    #[cfg(not(any(feature = "aws-lc-rs", feature = "ring")))]
    compile_error!("Please enable the `aws-lc-rs` or `ring` feature");

    #[cfg(feature = "aws-lc-rs")]
    use aws_lc_rs as crypto_backend;
    #[cfg(not(feature = "aws-lc-rs"))]
    use ring as crypto_backend;

    use crypto_backend::digest::{Context, SHA256};
    #[allow(dead_code)]
    pub(crate) struct Sha256(Context);

    #[allow(dead_code)]
    impl Sha256 {
        pub(crate) fn new() -> Self {
            Self(Context::new(&SHA256))
        }

        pub(crate) fn update(&mut self, chunk: &[u8]) {
            self.0.update(chunk);
        }

        pub(crate) fn finish(self) -> [u8; 32] {
            let digest = self.0.finish();
            digest.as_ref().try_into().expect("sha256 hash is 32 bytes")
        }
    }
}

#[cfg(target_arch = "wasm32")]
mod crypto_backend {
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
}

pub(crate) use crypto_backend::Sha256;
