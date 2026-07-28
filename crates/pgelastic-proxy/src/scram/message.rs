//! SCRAM message syntax (RFC 5802 §7, profiled by RFC 7677).
//!
//! Parsing is strict on purpose: every field this proxy relies on for an
//! authentication decision is validated here, so the exchange types below never
//! have to reason about malformed input.

use std::num::NonZeroU32;

use base64::Engine as _;

const B64: base64::engine::general_purpose::GeneralPurpose =
    base64::engine::general_purpose::STANDARD;

/// `PostgreSQL` caps the iteration count it will accept from a peer. Without a
/// ceiling, a hostile server names a billion iterations and the proxy burns a
/// CPU on its behalf.
pub const MAX_ITERATIONS: u32 = 1_000_000;

const MAX_SALT_LEN: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ScramError {
    #[error("malformed SCRAM message: {0}")]
    Malformed(&'static str),
    #[error("unsupported SCRAM channel-binding flag")]
    UnsupportedChannelBinding,
    #[error("SCRAM nonce does not match the one issued")]
    NonceMismatch,
    #[error("SCRAM iteration count {0} is outside the permitted range")]
    BadIterationCount(u32),
    #[error("server rejected the SCRAM exchange: {0}")]
    ServerError(String),
    #[error("server signature did not verify")]
    ServerSignatureMismatch,
}

type Result<T> = std::result::Result<T, ScramError>;

fn decode_b64(value: &str, what: &'static str) -> Result<Vec<u8>> {
    B64.decode(value).map_err(|_| ScramError::Malformed(what))
}

pub fn encode_b64(value: &[u8]) -> String {
    B64.encode(value)
}

/// Splits `k=v` on the first `=` only: base64 values contain padding `=`.
fn attribute(part: &str) -> Result<(char, &str)> {
    let mut chars = part.chars();
    let key = chars.next().ok_or(ScramError::Malformed("empty attribute"))?;
    let rest = chars.as_str();
    let value = rest
        .strip_prefix('=')
        .ok_or(ScramError::Malformed("attribute is not k=v"))?;
    Ok((key, value))
}

/// Whether the client claims to support channel binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelBinding {
    /// `n` — the client does not support it.
    NotSupported,
    /// `y` — the client supports it but saw no `-PLUS` mechanism on offer.
    SupportedButUnused,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientFirst {
    pub channel_binding: ChannelBinding,
    /// The literal gs2 header, needed to check the `c=` attribute later.
    pub gs2_header: String,
    /// `client-first-message-bare`, verbatim, for the auth message.
    pub bare: String,
    pub nonce: String,
}

impl ClientFirst {
    pub fn parse(input: &str) -> Result<Self> {
        let (flag, rest) = input
            .split_once(',')
            .ok_or(ScramError::Malformed("missing gs2 channel-binding flag"))?;
        let channel_binding = match flag {
            "n" => ChannelBinding::NotSupported,
            "y" => ChannelBinding::SupportedButUnused,
            _ => return Err(ScramError::UnsupportedChannelBinding),
        };
        let (authzid, bare) = rest
            .split_once(',')
            .ok_or(ScramError::Malformed("missing gs2 authzid field"))?;
        if !authzid.is_empty() {
            // PostgreSQL has no SASL authorization identity; accepting one here
            // would silently ignore a request to act as a different role.
            return Err(ScramError::Malformed("gs2 authzid must be empty"));
        }

        let mut nonce = None;
        let mut saw_username = false;
        for (index, part) in bare.split(',').enumerate() {
            let (key, value) = attribute(part)?;
            match key {
                'm' => return Err(ScramError::Malformed("mandatory extension not supported")),
                'n' if index == 0 => saw_username = true,
                'r' if index == 1 => nonce = Some(value.to_owned()),
                _ => {}
            }
        }
        if !saw_username {
            return Err(ScramError::Malformed("missing username attribute"));
        }
        let nonce = nonce.ok_or(ScramError::Malformed("missing client nonce"))?;
        if nonce.is_empty() {
            return Err(ScramError::Malformed("empty client nonce"));
        }

        Ok(Self {
            channel_binding,
            gs2_header: format!("{flag},{authzid},"),
            bare: bare.to_owned(),
            nonce,
        })
    }

    pub fn build(nonce: &str) -> (String, String) {
        let bare = format!("n=,r={nonce}");
        (format!("n,,{bare}"), bare)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerFirst {
    pub nonce: String,
    pub salt: Vec<u8>,
    pub iterations: NonZeroU32,
}

impl ServerFirst {
    pub fn parse(input: &str) -> Result<Self> {
        let mut nonce = None;
        let mut salt = None;
        let mut iterations = None;
        for part in input.split(',') {
            match attribute(part)? {
                ('r', value) => nonce = Some(value.to_owned()),
                ('s', value) => salt = Some(decode_b64(value, "server salt")?),
                ('i', value) => {
                    iterations = Some(
                        value
                            .parse::<u32>()
                            .map_err(|_| ScramError::Malformed("iteration count"))?,
                    );
                }
                ('m', _) => return Err(ScramError::Malformed("mandatory extension not supported")),
                _ => {}
            }
        }
        let nonce = nonce.ok_or(ScramError::Malformed("missing server nonce"))?;
        let salt = salt.ok_or(ScramError::Malformed("missing salt"))?;
        if salt.len() > MAX_SALT_LEN {
            return Err(ScramError::Malformed("salt is implausibly long"));
        }
        let iterations = iterations.ok_or(ScramError::Malformed("missing iteration count"))?;
        if iterations == 0 || iterations > MAX_ITERATIONS {
            return Err(ScramError::BadIterationCount(iterations));
        }
        Ok(Self {
            nonce,
            salt,
            iterations: NonZeroU32::new(iterations).expect("checked above"),
        })
    }

    pub fn build(nonce: &str, salt: &[u8], iterations: NonZeroU32) -> String {
        format!("r={nonce},s={},i={iterations}", encode_b64(salt))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientFinal {
    pub channel_binding: Vec<u8>,
    pub nonce: String,
    pub proof: Vec<u8>,
    /// `client-final-message-without-proof`, verbatim, for the auth message.
    pub without_proof: String,
}

impl ClientFinal {
    pub fn parse(input: &str) -> Result<Self> {
        let (without_proof, proof) = input
            .rsplit_once(",p=")
            .ok_or(ScramError::Malformed("missing client proof"))?;
        let proof = decode_b64(proof, "client proof")?;

        let mut channel_binding = None;
        let mut nonce = None;
        for part in without_proof.split(',') {
            match attribute(part)? {
                ('c', value) => channel_binding = Some(decode_b64(value, "channel binding")?),
                ('r', value) => nonce = Some(value.to_owned()),
                ('m', _) => return Err(ScramError::Malformed("mandatory extension not supported")),
                _ => {}
            }
        }
        Ok(Self {
            channel_binding: channel_binding
                .ok_or(ScramError::Malformed("missing channel-binding attribute"))?,
            nonce: nonce.ok_or(ScramError::Malformed("missing nonce"))?,
            proof,
            without_proof: without_proof.to_owned(),
        })
    }

    pub fn build_without_proof(gs2_header: &str, nonce: &str) -> String {
        format!("c={},r={nonce}", encode_b64(gs2_header.as_bytes()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerFinal {
    Verifier(Vec<u8>),
    Error(String),
}

impl ServerFinal {
    pub fn parse(input: &str) -> Result<Self> {
        for part in input.split(',') {
            match attribute(part)? {
                ('v', value) => return Ok(Self::Verifier(decode_b64(value, "server signature")?)),
                ('e', value) => return Ok(Self::Error(value.to_owned())),
                _ => {}
            }
        }
        Err(ScramError::Malformed("server-final has neither v= nor e="))
    }

    pub fn build_verifier(signature: &[u8]) -> String {
        format!("v={}", encode_b64(signature))
    }
}

pub fn auth_message(client_first_bare: &str, server_first: &str, client_final: &str) -> String {
    format!("{client_first_bare},{server_first},{client_final}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_rfc_7677_client_first_parses() {
        let parsed = ClientFirst::parse("n,,n=user,r=rOprNGfwEbeRWgbNEkqO").unwrap();
        assert_eq!(parsed.channel_binding, ChannelBinding::NotSupported);
        assert_eq!(parsed.gs2_header, "n,,");
        assert_eq!(parsed.bare, "n=user,r=rOprNGfwEbeRWgbNEkqO");
        assert_eq!(parsed.nonce, "rOprNGfwEbeRWgbNEkqO");
    }

    #[test]
    fn a_client_that_supports_channel_binding_but_saw_no_plus_is_accepted() {
        let parsed = ClientFirst::parse("y,,n=,r=abcd").unwrap();
        assert_eq!(parsed.channel_binding, ChannelBinding::SupportedButUnused);
        assert_eq!(parsed.gs2_header, "y,,");
    }

    #[test]
    fn a_channel_binding_client_is_refused_when_plus_is_not_offered() {
        assert_eq!(
            ClientFirst::parse("p=tls-server-end-point,,n=,r=abcd"),
            Err(ScramError::UnsupportedChannelBinding)
        );
    }

    #[test]
    fn an_authzid_is_refused() {
        assert!(ClientFirst::parse("n,a=other,n=,r=abcd").is_err());
    }

    #[test]
    fn the_rfc_7677_server_first_parses() {
        let parsed = ServerFirst::parse(
            "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096",
        )
        .unwrap();
        assert_eq!(parsed.iterations.get(), 4096);
        assert_eq!(parsed.salt.len(), 16);
        assert!(parsed.nonce.starts_with("rOprNGfwEbeRWgbNEkqO"));
    }

    #[test]
    fn an_absurd_iteration_count_is_refused_before_any_hashing() {
        assert_eq!(
            ServerFirst::parse("r=abc,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=2000000"),
            Err(ScramError::BadIterationCount(2_000_000))
        );
    }

    #[test]
    fn the_rfc_7677_client_final_parses_and_keeps_the_proofless_prefix() {
        let input = "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=";
        let parsed = ClientFinal::parse(input).unwrap();
        assert_eq!(parsed.channel_binding, b"n,,");
        assert_eq!(parsed.proof.len(), 32);
        assert_eq!(
            parsed.without_proof,
            "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0"
        );
    }

    #[test]
    fn a_base64_value_containing_padding_is_not_split_on_it() {
        let parsed = ServerFirst::parse("r=n,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096").unwrap();
        assert_eq!(parsed.salt.len(), 16);
    }

    #[test]
    fn server_final_distinguishes_a_signature_from_an_error() {
        assert_eq!(
            ServerFinal::parse("v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=").unwrap(),
            ServerFinal::Verifier(
                decode_b64("6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=", "x").unwrap()
            )
        );
        assert_eq!(
            ServerFinal::parse("e=invalid-proof").unwrap(),
            ServerFinal::Error("invalid-proof".to_owned())
        );
    }

    #[test]
    fn a_mandatory_extension_is_refused_rather_than_ignored() {
        assert!(ClientFirst::parse("n,,m=x,n=,r=abcd").is_err());
        assert!(ServerFirst::parse("m=x,r=a,s=YQ==,i=1").is_err());
    }
}
