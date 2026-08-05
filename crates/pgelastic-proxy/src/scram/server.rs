//! The proxy authenticating a client (RFC 5802 server role).

use subtle::ConstantTimeEq;

use super::crypto::{self, Key};
use super::message::{
    ClientFinal, ClientFirst, ScramError, ServerFinal, ServerFirst, auth_message, encode_b64,
};
use super::verifier::ScramVerifier;

/// The longest client nonce the exchange will carry. libpq sends 24 characters.
const MAX_CLIENT_NONCE: usize = 256;

/// How a completed exchange ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScramOutcome {
    /// The proof verified; the payload is the `server-final-message`.
    Verified(String),
    /// The proof did not verify. Carries no detail on purpose: a client that
    /// can tell "no such user" from "wrong password" can enumerate tenants.
    Rejected,
}

#[derive(Debug)]
enum State {
    AwaitingClientFirst,
    AwaitingClientFinal {
        client_first_bare: String,
        server_first: String,
        gs2_header: String,
        nonce: String,
    },
    Done,
}

#[derive(Debug)]
pub struct ScramServer {
    verifier: ScramVerifier,
    server_nonce: String,
    state: State,
}

impl ScramServer {
    pub fn new(verifier: ScramVerifier, server_nonce: String) -> Self {
        Self {
            verifier,
            server_nonce,
            state: State::AwaitingClientFirst,
        }
    }

    /// Consumes `client-first-message` and produces `server-first-message`.
    pub fn server_first(&mut self, client_first: &[u8]) -> Result<String, ScramError> {
        let State::AwaitingClientFirst = self.state else {
            return Err(ScramError::Malformed("unexpected SCRAM message"));
        };
        let text =
            std::str::from_utf8(client_first).map_err(|_| ScramError::Malformed("not UTF-8"))?;
        let parsed = ClientFirst::parse(text)?;
        // The client's nonce is echoed into the combined nonce, into server-first, and kept in
        // the state until the exchange finishes - so an unauthenticated peer that sends a long
        // one has the proxy hold several copies of it, per connection, before proving anything.
        // libpq sends 24 characters; PostgreSQL refuses an authentication token over 10 000
        // bytes outright. Refusing here bounds the exchange rather than the connection.
        if parsed.nonce.len() > MAX_CLIENT_NONCE {
            return Err(ScramError::Malformed("client nonce is too long"));
        }

        let nonce = format!("{}{}", parsed.nonce, self.server_nonce);
        let server_first =
            ServerFirst::build(&nonce, &self.verifier.salt, self.verifier.iterations);
        self.state = State::AwaitingClientFinal {
            client_first_bare: parsed.bare,
            server_first: server_first.clone(),
            gs2_header: parsed.gs2_header,
            nonce,
        };
        Ok(server_first)
    }

    /// Consumes `client-final-message` and decides the exchange.
    pub fn finish(&mut self, client_final: &[u8]) -> Result<ScramOutcome, ScramError> {
        let State::AwaitingClientFinal {
            client_first_bare,
            server_first,
            gs2_header,
            nonce,
        } = std::mem::replace(&mut self.state, State::Done)
        else {
            return Err(ScramError::Malformed("unexpected SCRAM message"));
        };

        let text =
            std::str::from_utf8(client_final).map_err(|_| ScramError::Malformed("not UTF-8"))?;
        let parsed = ClientFinal::parse(text)?;

        if parsed.nonce != nonce {
            return Err(ScramError::NonceMismatch);
        }
        // The client echoes the gs2 header it sent, so a downgrade attempt that
        // rewrote the channel-binding flag in flight shows up here.
        if parsed.channel_binding != gs2_header.as_bytes() {
            return Err(ScramError::Malformed("channel-binding data was altered"));
        }

        let auth = auth_message(&client_first_bare, &server_first, &parsed.without_proof);
        let Ok(proof) = Key::try_from(parsed.proof.as_slice()) else {
            return Ok(ScramOutcome::Rejected);
        };

        let client_signature = crypto::hmac_sha256(&self.verifier.stored_key, auth.as_bytes());
        let client_key = crypto::xor(&proof, &client_signature);
        let candidate = crypto::sha256(&client_key);

        if bool::from(candidate.ct_eq(&self.verifier.stored_key)) {
            let signature = crypto::hmac_sha256(&self.verifier.server_key, auth.as_bytes());
            Ok(ScramOutcome::Verified(ServerFinal::build_verifier(
                &signature,
            )))
        } else {
            Ok(ScramOutcome::Rejected)
        }
    }
}

/// The `e=` form of `server-final-message`, for a failure the client should see
/// as a SCRAM error rather than a transport fault.
pub fn server_error(reason: &str) -> String {
    format!("e={reason}")
}

/// Base64 of a value, exposed for callers assembling their own messages.
pub fn b64(value: &[u8]) -> String {
    encode_b64(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scram::client::ScramClient;
    use crate::scram::crypto::salted_password_blocking;
    use crate::scram::verifier::{DEFAULT_ITERATIONS, MockSecret};
    use std::num::NonZeroU32;
    use zeroize::Zeroizing;

    fn iterations() -> NonZeroU32 {
        NonZeroU32::new(DEFAULT_ITERATIONS).unwrap()
    }

    // The client's nonce is echoed into the combined nonce, into server-first, and held in
    // the state until the exchange ends, so an unauthenticated peer sending a long one has the
    // proxy hold several copies of it per connection before proving anything at all.
    #[test]
    fn a_client_nonce_longer_than_any_client_sends_is_refused() {
        let verifier = ScramVerifier::generate("hunter2").unwrap();
        let mut server = ScramServer::new(verifier, "server-nonce-xyz".to_owned());
        let nonce = "a".repeat(MAX_CLIENT_NONCE + 1);
        let client_first = format!("n,,n=user,r={nonce}");

        let refused = server.server_first(client_first.as_bytes());

        assert!(
            matches!(refused, Err(ScramError::Malformed(_))),
            "a {} byte nonce was accepted: {refused:?}",
            nonce.len()
        );
    }

    fn exchange(verifier: ScramVerifier, password: &str) -> Result<ScramOutcome, ScramError> {
        let mut server = ScramServer::new(verifier, "server-nonce-xyz".to_owned());
        let mut client = ScramClient::new("client-nonce-abc".to_owned());

        let client_first = client.client_first();
        let server_first = server.server_first(client_first.as_bytes())?;
        let parsed = ServerFirst::parse(&server_first)?;
        let salted = salted_password_blocking(password, &parsed.salt, parsed.iterations);
        let client_final = client.client_final(server_first.as_bytes(), &salted)?;
        server.finish(client_final.as_bytes())
    }

    #[test]
    fn the_right_password_verifies_and_signs_back() {
        let verifier = ScramVerifier::generate("hunter2").unwrap();
        let outcome = exchange(verifier, "hunter2").unwrap();
        assert!(matches!(outcome, ScramOutcome::Verified(_)));
    }

    #[test]
    fn the_wrong_password_is_rejected() {
        let verifier = ScramVerifier::generate("hunter2").unwrap();
        assert_eq!(
            exchange(verifier, "hunter3").unwrap(),
            ScramOutcome::Rejected
        );
    }

    #[test]
    fn an_unknown_user_is_rejected_identically_to_a_wrong_password() {
        let real = ScramVerifier::generate("hunter2").unwrap();
        let mock = MockSecret::generate()
            .unwrap()
            .verifier_for(b"nobody", iterations());
        assert_eq!(
            exchange(real, "wrong").unwrap(),
            exchange(mock, "wrong").unwrap()
        );
    }

    #[test]
    fn a_tampered_channel_binding_flag_is_caught() {
        let verifier = ScramVerifier::generate("hunter2").unwrap();
        let mut server = ScramServer::new(verifier, "server-nonce-xyz".to_owned());
        let server_first = server.server_first(b"n,,n=,r=client").unwrap();
        let parsed = ServerFirst::parse(&server_first).unwrap();
        // c=eSws is base64("y,,"), which is not the header the client sent.
        let final_message = format!("c=eSws,r={},p={}", parsed.nonce, encode_b64(&[0u8; 32]));
        assert_eq!(
            server.finish(final_message.as_bytes()),
            Err(ScramError::Malformed("channel-binding data was altered"))
        );
    }

    #[test]
    fn a_replayed_nonce_from_another_exchange_is_caught() {
        let verifier = ScramVerifier::generate("hunter2").unwrap();
        let mut server = ScramServer::new(verifier, "server-nonce-xyz".to_owned());
        server.server_first(b"n,,n=,r=client").unwrap();
        let final_message = format!("c=biws,r=someone-elses-nonce,p={}", encode_b64(&[0u8; 32]));
        assert_eq!(
            server.finish(final_message.as_bytes()),
            Err(ScramError::NonceMismatch)
        );
    }

    #[test]
    fn messages_out_of_order_are_refused() {
        let verifier = ScramVerifier::generate("hunter2").unwrap();
        let mut server = ScramServer::new(verifier, "n".to_owned());
        assert!(server.finish(b"c=biws,r=x,p=AA==").is_err());
    }

    #[test]
    fn a_short_proof_is_rejected_rather_than_panicking() {
        let verifier = ScramVerifier::generate("hunter2").unwrap();
        let mut server = ScramServer::new(verifier, "server-nonce-xyz".to_owned());
        let server_first = server.server_first(b"n,,n=,r=client").unwrap();
        let parsed = ServerFirst::parse(&server_first).unwrap();
        let final_message = format!("c=biws,r={},p={}", parsed.nonce, encode_b64(b"short"));
        assert_eq!(
            server.finish(final_message.as_bytes()).unwrap(),
            ScramOutcome::Rejected
        );
    }

    #[test]
    fn the_client_verifies_the_servers_signature() {
        let verifier = ScramVerifier::generate("hunter2").unwrap();
        let mut server = ScramServer::new(verifier, "server-nonce-xyz".to_owned());
        let mut client = ScramClient::new("client-nonce-abc".to_owned());

        let client_first = client.client_first();
        let server_first = server.server_first(client_first.as_bytes()).unwrap();
        let parsed = ServerFirst::parse(&server_first).unwrap();
        let salted: Zeroizing<Key> =
            salted_password_blocking("hunter2", &parsed.salt, parsed.iterations);
        let client_final = client
            .client_final(server_first.as_bytes(), &salted)
            .unwrap();
        let ScramOutcome::Verified(server_final) = server.finish(client_final.as_bytes()).unwrap()
        else {
            panic!("expected the exchange to verify");
        };
        client.verify_server_final(server_final.as_bytes()).unwrap();
    }
}
