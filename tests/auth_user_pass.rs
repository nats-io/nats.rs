mod util;
pub use util::*;

#[test]
fn basic_user_pass_auth() {
    let s = util::run_server("tests/configs/user_pass.conf");

    assert!(nats::connect(&s.client_url()).is_err());

    assert!(nats::Options::with_user_pass("derek", "s3cr3t")
        .connect(&s.client_url())
        .is_ok());

    assert!(nats::connect(&s.client_url_with("derek", "s3cr3t")).is_ok());

    // Check override.
    assert!(nats::Options::with_user_pass("derek", "bad-password")
        .connect(&s.client_url_with("derek", "s3cr3t"))
        .is_ok());
}
