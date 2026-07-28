//! The proxy authenticating to a `PostgreSQL` backend (RFC 5802 client role).
//!
//! The expensive step — deriving `SaltedPassword` — is deliberately *not* done
//! here. [`client_final`](ScramClient::client_final) takes an already-derived
//! value so the caller is forced to run PBKDF2 through
//! [`KdfPool`](super::crypto::KdfPool) rather than on a runtime worker.

use subtle::ConstantTimeEq;
use zeroize::Zeroizing;

use super::crypto::{self, Key};
use super::message::{
    ClientFinal, ClientFirst, ScramError, ServerFinal, ServerFirst, auth_message, encode_b64,
};

#[derive(Debug)]
enum State {
    Initial,
    AwaitingServerFirst {
        bare: String,
        gs2_header: String,
        nonce: String,
    },
    AwaitingServerFinal {
        server_key: Key,
        auth_message: String,
    },
    Done,
}

#[derive(Debug)]
pub struct ScramClient {
    nonce: String,
    state: State,
}

impl ScramClient {
    pub fn new(nonce: String) -> Self {
        Self {
            nonce,
            state: State::Initial,
        }
    }

    /// `client-first-message`.
    ///
    /// The username field is left empty, as `PostgreSQL` requires: the role is
    /// carried in the startup packet, and a SASL username would be ignored.
    pub fn client_first(&mut self) -> String {
        let (full, bare) = ClientFirst::build(&self.nonce);
        self.state = State::AwaitingServerFirst {
            bare,
            gs2_header: "n,,".to_owned(),
            nonce: self.nonce.clone(),
        };
        full
    }

    /// The salt and iteration count the server chose, so the caller can derive
    /// `SaltedPassword` off the executor before calling
    /// [`client_final`](Self::client_final).
    pub fn parse_server_first(server_first: &[u8]) -> Result<ServerFirst, ScramError> {
        let text =
            std::str::from_utf8(server_first).map_err(|_| ScramError::Malformed("not UTF-8"))?;
        ServerFirst::parse(text)
    }

    /// `client-final-message`.
    pub fn client_final(
        &mut self,
        server_first: &[u8],
        salted_password: &Zeroizing<Key>,
    ) -> Result<String, ScramError> {
        let State::AwaitingServerFirst {
            bare,
            gs2_header,
            nonce,
        } = std::mem::replace(&mut self.state, State::Done)
        else {
            return Err(ScramError::Malformed("unexpected SCRAM message"));
        };

        let text =
            std::str::from_utf8(server_first).map_err(|_| ScramError::Malformed("not UTF-8"))?;
        let parsed = ServerFirst::parse(text)?;
        if !parsed.nonce.starts_with(&nonce) || parsed.nonce.len() == nonce.len() {
            return Err(ScramError::NonceMismatch);
        }

        let without_proof = ClientFinal::build_without_proof(&gs2_header, &parsed.nonce);
        let auth = auth_message(&bare, text, &without_proof);

        let client_key = crypto::hmac_sha256(salted_password.as_ref(), b"Client Key");
        let stored_key = crypto::sha256(&client_key);
        let client_signature = crypto::hmac_sha256(&stored_key, auth.as_bytes());
        let proof = crypto::xor(&client_key, &client_signature);

        self.state = State::AwaitingServerFinal {
            server_key: crypto::hmac_sha256(salted_password.as_ref(), b"Server Key"),
            auth_message: auth,
        };
        Ok(format!("{without_proof},p={}", encode_b64(&proof)))
    }

    /// Checks `server-final-message`.
    ///
    /// Skipping this turns SCRAM into a one-way protocol: a man in the middle
    /// that never had the verifier could still complete the handshake.
    pub fn verify_server_final(&mut self, server_final: &[u8]) -> Result<(), ScramError> {
        let State::AwaitingServerFinal {
            server_key,
            auth_message,
        } = std::mem::replace(&mut self.state, State::Done)
        else {
            return Err(ScramError::Malformed("unexpected SCRAM message"));
        };
        let text =
            std::str::from_utf8(server_final).map_err(|_| ScramError::Malformed("not UTF-8"))?;
        match ServerFinal::parse(text)? {
            ServerFinal::Error(reason) => Err(ScramError::ServerError(reason)),
            ServerFinal::Verifier(signature) => {
                let expected = crypto::hmac_sha256(&server_key, auth_message.as_bytes());
                if bool::from(signature.as_slice().ct_eq(&expected[..])) {
                    Ok(())
                } else {
                    Err(ScramError::ServerSignatureMismatch)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scram::crypto::salted_password_blocking;
    use std::num::NonZeroU32;

    fn salted() -> Zeroizing<Key> {
        salted_password_blocking("pencil", b"saltsaltsaltsalt", NonZeroU32::new(1).unwrap())
    }

    #[test]
    fn the_rfc_7677_client_side_reproduces_the_published_proof() {
        let mut client = ScramClient::new("rOprNGfwEbeRWgbNEkqO".to_owned());
        client.state = State::AwaitingServerFirst {
            bare: "n=user,r=rOprNGfwEbeRWgbNEkqO".to_owned(),
            gs2_header: "n,,".to_owned(),
            nonce: "rOprNGfwEbeRWgbNEkqO".to_owned(),
        };
        let server_first = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        let salt = {
            use base64::Engine as _;
            base64::engine::general_purpose::STANDARD
                .decode("W22ZaJ0SNY7soEsUEjb6gQ==")
                .unwrap()
        };
        let salted = salted_password_blocking("pencil", &salt, NonZeroU32::new(4096).unwrap());
        let final_message = client
            .client_final(server_first.as_bytes(), &salted)
            .unwrap();
        assert_eq!(
            final_message,
            "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,\
             p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
        );
        client
            .verify_server_final(b"v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=")
            .unwrap();
    }

    #[test]
    fn a_server_that_does_not_extend_the_client_nonce_is_refused() {
        let mut client = ScramClient::new("clientnonce".to_owned());
        let first = client.client_first();
        assert!(first.starts_with("n,,n=,r=clientnonce"));
        let server_first = "r=clientnonce,s=c2FsdA==,i=4096";
        assert_eq!(
            client.client_final(server_first.as_bytes(), &salted()),
            Err(ScramError::NonceMismatch)
        );
    }

    #[test]
    fn a_server_nonce_that_does_not_carry_ours_is_refused() {
        let mut client = ScramClient::new("clientnonce".to_owned());
        client.client_first();
        let server_first = "r=somethingelse,s=c2FsdA==,i=4096";
        assert_eq!(
            client.client_final(server_first.as_bytes(), &salted()),
            Err(ScramError::NonceMismatch)
        );
    }

    #[test]
    fn a_forged_server_signature_is_refused() {
        let mut client = ScramClient::new("clientnonce".to_owned());
        client.client_first();
        client
            .client_final(b"r=clientnonceSERVER,s=c2FsdA==,i=4096", &salted())
            .unwrap();
        assert_eq!(
            client.verify_server_final(&format!("v={}", encode_b64(&[0u8; 32])).into_bytes()),
            Err(ScramError::ServerSignatureMismatch)
        );
    }

    #[test]
    fn a_server_reported_error_surfaces_verbatim() {
        let mut client = ScramClient::new("clientnonce".to_owned());
        client.client_first();
        client
            .client_final(b"r=clientnonceSERVER,s=c2FsdA==,i=4096", &salted())
            .unwrap();
        assert_eq!(
            client.verify_server_final(b"e=invalid-proof"),
            Err(ScramError::ServerError("invalid-proof".to_owned()))
        );
    }
}
