use crate::{options::CallbackArg1, AuthError};

#[derive(Default)]
pub struct Auth {
    pub jwt: Option<String>,
    pub nkey: Option<String>,
    pub(crate) signature_callback: Option<CallbackArg1<String, Result<String, AuthError>>>,
    pub signature: Option<Vec<u8>>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub token: Option<String>,
}

impl Auth {
    pub fn new() -> Auth {
        Auth::default()
    }
}
