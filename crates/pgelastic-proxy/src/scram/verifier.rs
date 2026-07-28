//! Stored SCRAM verifiers, and the mock ones that keep unknown users
//! indistinguishable from wrong passwords.

use std::num::NonZeroU32;

use zeroize::{Zeroize, ZeroizeOnDrop};

use super::crypto::{self, KEY_LEN, Key};
use super::message::{MAX_ITERATIONS, encode_b64};

/// What `PostgreSQL` uses when `password_encryption = scram-sha-256`.
pub const DEFAULT_ITERATIONS: u32 = 4096;
pub const SALT_LEN: usize = 16;

const PREFIX: &str = "SCRAM-SHA-256$";

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum VerifierError {
    #[error("verifier is not a SCRAM-SHA-256 secret")]
    WrongMechanism,
    #[error("verifier is malformed: {0}")]
    Malformed(&'static str),
    #[error("verifier iteration count {0} is outside the permitted range")]
    BadIterationCount(u32),
}

/// A `PostgreSQL` `rolpassword` SCRAM secret.
///
/// Holds `StoredKey` and `ServerKey`, never the password: possession of these
/// is enough to *verify* a client but not to impersonate one to another server,
/// which is the entire point of SCRAM.
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct ScramVerifier {
    #[zeroize(skip)]
    pub iterations: NonZeroU32,
    pub salt: Vec<u8>,
    pub stored_key: Key,
    pub server_key: Key,
}

impl std::fmt::Debug for ScramVerifier {
    /// Redacted: a `Debug` print of a verifier in a log is a credential leak.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScramVerifier")
            .field("iterations", &self.iterations)
            .field("salt_len", &self.salt.len())
            .finish_non_exhaustive()
    }
}

impl ScramVerifier {
    pub fn from_password(password: &str, salt: Vec<u8>, iterations: NonZeroU32) -> Self {
        let salted = crypto::salted_password_blocking(password, &salt, iterations);
        let client_key = crypto::hmac_sha256(salted.as_ref(), b"Client Key");
        Self {
            iterations,
            salt,
            stored_key: crypto::sha256(&client_key),
            server_key: crypto::hmac_sha256(salted.as_ref(), b"Server Key"),
        }
    }

    pub fn generate(password: &str) -> crate::Result<Self> {
        let salt = crypto::random_bytes::<SALT_LEN>()?.to_vec();
        Ok(Self::from_password(
            password,
            salt,
            NonZeroU32::new(DEFAULT_ITERATIONS).expect("nonzero"),
        ))
    }

    /// Parses `SCRAM-SHA-256$<iterations>:<salt>$<StoredKey>:<ServerKey>`.
    pub fn parse(secret: &str) -> Result<Self, VerifierError> {
        let body = secret
            .strip_prefix(PREFIX)
            .ok_or(VerifierError::WrongMechanism)?;
        let (params, keys) = body
            .split_once('$')
            .ok_or(VerifierError::Malformed("missing key section"))?;
        let (iterations, salt) = params
            .split_once(':')
            .ok_or(VerifierError::Malformed("missing salt"))?;
        let (stored_key, server_key) = keys
            .split_once(':')
            .ok_or(VerifierError::Malformed("missing server key"))?;

        let iterations: u32 = iterations
            .parse()
            .map_err(|_| VerifierError::Malformed("iteration count"))?;
        if iterations == 0 || iterations > MAX_ITERATIONS {
            return Err(VerifierError::BadIterationCount(iterations));
        }

        Ok(Self {
            iterations: NonZeroU32::new(iterations).expect("checked above"),
            salt: decode_key_material(salt, "salt")?,
            stored_key: fixed_key(&decode_key_material(stored_key, "stored key")?)?,
            server_key: fixed_key(&decode_key_material(server_key, "server key")?)?,
        })
    }

    /// Renders the verifier in `PostgreSQL`'s `rolpassword` form.
    pub fn to_secret(&self) -> String {
        format!(
            "{PREFIX}{}:{}${}:{}",
            self.iterations,
            encode_b64(&self.salt),
            encode_b64(&self.stored_key),
            encode_b64(&self.server_key)
        )
    }

    /// A verifier for a user that does not exist.
    ///
    /// Structurally indistinguishable from a real one — same iteration count,
    /// same salt length, and derived deterministically so a user that is
    /// probed twice gets the same salt both times. Because it is derived under
    /// a per-process secret that never leaves memory, no proof can ever match
    /// it, so the exchange fails at exactly the same point, in exactly the same
    /// way, and after exactly the same work as a wrong password.
    pub fn mock(username: &[u8], process_secret: &Key, iterations: NonZeroU32) -> Self {
        let derive = |domain: &[u8]| {
            let mut input = Vec::with_capacity(domain.len() + username.len());
            input.extend_from_slice(domain);
            input.extend_from_slice(username);
            crypto::hmac_sha256(process_secret, &input)
        };
        Self {
            iterations,
            salt: derive(b"pgelastic mock salt")[..SALT_LEN].to_vec(),
            stored_key: derive(b"pgelastic mock stored key"),
            server_key: derive(b"pgelastic mock server key"),
        }
    }
}

fn decode_key_material(value: &str, what: &'static str) -> Result<Vec<u8>, VerifierError> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .decode(value)
        .map_err(|_| VerifierError::Malformed(what))
}

fn fixed_key(bytes: &[u8]) -> Result<Key, VerifierError> {
    Key::try_from(bytes).map_err(|_| VerifierError::Malformed("key is not 32 bytes"))
}

/// The per-process secret mock verifiers are derived under.
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct MockSecret(Key);

impl std::fmt::Debug for MockSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MockSecret(<redacted>)")
    }
}

impl MockSecret {
    pub fn generate() -> crate::Result<Self> {
        Ok(Self(crypto::random_bytes::<KEY_LEN>()?))
    }

    pub fn verifier_for(&self, username: &[u8], iterations: NonZeroU32) -> ScramVerifier {
        ScramVerifier::mock(username, &self.0, iterations)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn iterations() -> NonZeroU32 {
        NonZeroU32::new(DEFAULT_ITERATIONS).unwrap()
    }

    #[test]
    fn a_generated_verifier_round_trips_through_the_postgres_secret_format() {
        let verifier = ScramVerifier::generate("hunter2").unwrap();
        let parsed = ScramVerifier::parse(&verifier.to_secret()).unwrap();
        assert_eq!(parsed.iterations, verifier.iterations);
        assert_eq!(parsed.salt, verifier.salt);
        assert_eq!(parsed.stored_key, verifier.stored_key);
        assert_eq!(parsed.server_key, verifier.server_key);
    }

    #[test]
    fn a_secret_from_postgres_parses() {
        let secret = "SCRAM-SHA-256$4096:c2FsdHNhbHRzYWx0c2FsdA==$\
                      WG5d8oPm3OtcPnkdi4Uo7BkeZkBFzpcXkuLmtbsT4qY=:\
                      WG5d8oPm3OtcPnkdi4Uo7BkeZkBFzpcXkuLmtbsT4qY=";
        let parsed = ScramVerifier::parse(secret).unwrap();
        assert_eq!(parsed.iterations.get(), 4096);
        assert_eq!(parsed.salt.len(), 16);
    }

    #[test]
    fn a_non_scram_secret_is_refused() {
        assert_eq!(
            ScramVerifier::parse("md5abcdef"),
            Err(VerifierError::WrongMechanism)
        );
    }

    #[test]
    fn a_mock_verifier_is_shaped_exactly_like_a_real_one() {
        let secret = MockSecret::generate().unwrap();
        let mock = secret.verifier_for(b"nobody", iterations());
        let real = ScramVerifier::generate("hunter2").unwrap();
        assert_eq!(mock.salt.len(), real.salt.len());
        assert_eq!(mock.iterations, real.iterations);
        assert_eq!(mock.stored_key.len(), real.stored_key.len());
        assert!(ScramVerifier::parse(&mock.to_secret()).is_ok());
    }

    #[test]
    fn a_mock_verifier_is_stable_per_user_and_distinct_across_users() {
        let secret = MockSecret::generate().unwrap();
        let once = secret.verifier_for(b"nobody", iterations());
        let twice = secret.verifier_for(b"nobody", iterations());
        let other = secret.verifier_for(b"somebody", iterations());
        assert_eq!(once.salt, twice.salt);
        assert_eq!(once.stored_key, twice.stored_key);
        assert_ne!(once.salt, other.salt);
    }

    #[test]
    fn mock_verifiers_differ_between_processes() {
        let a = MockSecret::generate().unwrap();
        let b = MockSecret::generate().unwrap();
        assert_ne!(
            a.verifier_for(b"nobody", iterations()).stored_key,
            b.verifier_for(b"nobody", iterations()).stored_key
        );
    }

    #[test]
    fn a_zero_iteration_verifier_is_refused() {
        let secret = "SCRAM-SHA-256$0:YQ==$YQ==:YQ==";
        assert_eq!(
            ScramVerifier::parse(secret),
            Err(VerifierError::BadIterationCount(0))
        );
    }
}
