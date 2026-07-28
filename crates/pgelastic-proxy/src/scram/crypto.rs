//! The primitives SCRAM-SHA-256 is built from.
//!
//! All of them come from `aws-lc-rs`, which rustls already pulls in as its
//! crypto provider: one audited implementation for the whole binary rather than
//! a second, separately-audited stack for password hashing.

use std::num::NonZeroU32;
use std::sync::Arc;

use aws_lc_rs::{digest, hmac, pbkdf2, rand};
use tokio::sync::Semaphore;
use zeroize::Zeroizing;

use crate::error::{ProxyError, Result};

pub const KEY_LEN: usize = 32;

pub type Key = [u8; KEY_LEN];

pub fn sha256(data: &[u8]) -> Key {
    let mut out = [0u8; KEY_LEN];
    out.copy_from_slice(digest::digest(&digest::SHA256, data).as_ref());
    out
}

pub fn hmac_sha256(key: &[u8], data: &[u8]) -> Key {
    let mut out = [0u8; KEY_LEN];
    out.copy_from_slice(hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), data).as_ref());
    out
}

pub fn xor(a: &Key, b: &Key) -> Key {
    let mut out = [0u8; KEY_LEN];
    for i in 0..KEY_LEN {
        out[i] = a[i] ^ b[i];
    }
    out
}

pub fn random_bytes<const N: usize>() -> Result<[u8; N]> {
    let mut out = [0u8; N];
    rand::fill(&mut out).map_err(|_| ProxyError::config("system randomness is unavailable"))?;
    Ok(out)
}

/// A SCRAM nonce: printable ASCII with no comma, which base64 satisfies.
pub fn random_nonce() -> Result<String> {
    use base64::Engine as _;
    let raw = random_bytes::<18>()?;
    Ok(base64::engine::general_purpose::STANDARD.encode(raw))
}

fn pbkdf2_sha256(password: &[u8], salt: &[u8], iterations: NonZeroU32) -> Zeroizing<Key> {
    let mut out = Zeroizing::new([0u8; KEY_LEN]);
    pbkdf2::derive(
        pbkdf2::PBKDF2_HMAC_SHA256,
        iterations,
        salt,
        password,
        out.as_mut(),
    );
    out
}

/// Applies SASLprep (RFC 4013) the way `PostgreSQL` does.
///
/// A password that SASLprep rejects is passed through byte-for-byte rather than
/// erroring, which is what the server does and therefore the only behaviour
/// that produces a matching verifier.
pub fn saslprep(password: &str) -> Zeroizing<String> {
    match stringprep::saslprep(password) {
        Ok(prepared) => Zeroizing::new(prepared.into_owned()),
        Err(_) => Zeroizing::new(password.to_owned()),
    }
}

/// Runs PBKDF2 off the async executor, behind a concurrency limit.
///
/// PBKDF2 at the `PostgreSQL` default of 4096 iterations is milliseconds of
/// solid CPU. On a runtime worker that stalls every other connection on the
/// same thread, and an unauthenticated peer choosing when to trigger it turns
/// that into a denial-of-service primitive. The permit count is the rate
/// limiter: excess authentications wait rather than multiplying the CPU cost.
#[derive(Debug, Clone)]
pub struct KdfPool {
    permits: Arc<Semaphore>,
}

impl KdfPool {
    pub fn new(concurrency: usize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(concurrency.max(1))),
        }
    }

    pub fn available(&self) -> usize {
        self.permits.available_permits()
    }

    /// `SaltedPassword := Hi(Normalize(password), salt, i)`.
    pub async fn salted_password(
        &self,
        password: Zeroizing<String>,
        salt: Vec<u8>,
        iterations: NonZeroU32,
    ) -> Result<Zeroizing<Key>> {
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ProxyError::ShuttingDown)?;
        let derived = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let prepared = saslprep(&password);
            pbkdf2_sha256(prepared.as_bytes(), &salt, iterations)
        })
        .await
        .map_err(|_| ProxyError::config("password hashing task failed"))?;
        Ok(derived)
    }
}

/// Blocking form of [`KdfPool::salted_password`], for start-up config loading
/// where there is no runtime to protect yet.
pub fn salted_password_blocking(
    password: &str,
    salt: &[u8],
    iterations: NonZeroU32,
) -> Zeroizing<Key> {
    let prepared = saslprep(password);
    pbkdf2_sha256(prepared.as_bytes(), salt, iterations)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The full RFC 7677 §3 exchange, driven through the primitives alone.
    #[test]
    fn the_rfc_7677_vector_reproduces_both_proofs() {
        use base64::Engine as _;
        let b64 = base64::engine::general_purpose::STANDARD;

        let client_first_bare = "n=user,r=rOprNGfwEbeRWgbNEkqO";
        let server_first = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        let client_final_without_proof =
            "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0";
        let auth_message =
            format!("{client_first_bare},{server_first},{client_final_without_proof}");

        let salt = b64.decode("W22ZaJ0SNY7soEsUEjb6gQ==").unwrap();
        let salted = salted_password_blocking("pencil", &salt, NonZeroU32::new(4096).unwrap());

        let client_key = hmac_sha256(salted.as_ref(), b"Client Key");
        let stored_key = sha256(&client_key);
        let client_signature = hmac_sha256(&stored_key, auth_message.as_bytes());
        assert_eq!(
            b64.encode(xor(&client_key, &client_signature)),
            "dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
        );

        let server_key = hmac_sha256(salted.as_ref(), b"Server Key");
        assert_eq!(
            b64.encode(hmac_sha256(&server_key, auth_message.as_bytes())),
            "6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="
        );
    }

    #[test]
    fn xor_is_its_own_inverse() {
        let a = sha256(b"a");
        let b = sha256(b"b");
        assert_eq!(xor(&xor(&a, &b), &b), a);
    }

    #[tokio::test]
    async fn the_kdf_pool_bounds_concurrency() {
        let pool = KdfPool::new(2);
        assert_eq!(pool.available(), 2);
        let salted = pool
            .salted_password(
                Zeroizing::new("pencil".to_owned()),
                b"salt".to_vec(),
                NonZeroU32::new(1).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(salted.len(), KEY_LEN);
        assert_eq!(pool.available(), 2);
    }
}
