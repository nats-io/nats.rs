use crate::{options::CallbackArg1, AuthError};

#[derive(Default)]
pub(crate) struct Auth {
    pub(crate) jwt: Option<String>,
    pub(crate) nkey: Option<String>,
    pub(crate) signature: Option<CallbackArg1<String, Result<String, AuthError>>>,
    pub(crate) username: Option<String>,
    pub(crate) password: Option<String>,
    pub(crate) token: Option<String>,
}
