//! Internal bearer-token storage.

use secrecy::{ExposeSecret, SecretString};
use tokio::sync::RwLock;

#[derive(Debug, Default)]
pub(crate) struct TokenStore {
    token: RwLock<Option<SecretString>>,
}

impl TokenStore {
    pub(crate) async fn set(&self, token: String) {
        *self.token.write().await = Some(SecretString::from(token));
    }

    pub(crate) async fn snapshot(&self) -> Option<String> {
        self.token
            .read()
            .await
            .as_ref()
            .map(|token| token.expose_secret().to_owned())
    }
}
