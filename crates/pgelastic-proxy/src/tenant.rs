//! Which tenant a new connection belongs to.
//!
//! The pool's Service is one endpoint in front of every tenant it holds, so the
//! tenant has to be read out of the connection itself. Everything downstream —
//! the routing table that says which instance holds the data, the capacity
//! account the checkout is charged to, the quiesce gate a cutover closes — is
//! keyed on the answer, so getting it wrong routes one customer's queries into
//! another customer's database.
//!
//! Disagreement is therefore refused rather than resolved. A client that names
//! one tenant in its startup options and a different one in its database name is
//! not a client with a preference; it is a client whose intent is unknown, and
//! `discriminatorPrecedence: Strict` is the API saying so.

use pgelastic_wire::StartupMessage;

use crate::config::{RoutingConfig, TenantDiscriminator};
use crate::error::{ProxyError, Result};

/// Reads a connection's tenant out of its startup packet.
#[derive(Debug, Clone)]
pub struct TenantResolver {
    discriminators: Vec<TenantDiscriminator>,
    option_key: String,
}

impl TenantResolver {
    pub fn new(routing: &RoutingConfig) -> Self {
        Self {
            discriminators: routing.tenant_discriminators.clone(),
            option_key: routing.startup_option_key.clone(),
        }
    }

    /// The tenant this connection belongs to.
    ///
    /// Falls back to the login role when no configured discriminator produced a
    /// candidate. That is not a guess: a startup packet with no database is one
    /// where `PostgreSQL` itself defaults the database to the role, so the role
    /// is what the connection actually names.
    pub fn resolve(&self, startup: &StartupMessage, role: &str) -> Result<String> {
        let mut decided: Option<(TenantDiscriminator, String)> = None;
        for discriminator in &self.discriminators {
            let Some(candidate) = self.candidate(*discriminator, startup, role) else {
                continue;
            };
            match &decided {
                None => decided = Some((*discriminator, candidate)),
                Some((first, agreed)) if *agreed != candidate => {
                    return Err(ProxyError::client(format!(
                        "this connection names tenant {agreed:?} by {first:?} and \
                         {candidate:?} by {discriminator:?}; pgelastic refuses a connection \
                         whose tenant is ambiguous rather than choosing one of them"
                    )));
                }
                Some(_) => {}
            }
        }
        Ok(decided.map_or_else(|| role.to_owned(), |(_, tenant)| tenant))
    }

    fn candidate(
        &self,
        discriminator: TenantDiscriminator,
        startup: &StartupMessage,
        role: &str,
    ) -> Option<String> {
        let text = |name: &[u8]| {
            startup
                .get(name)
                .map(|value| String::from_utf8_lossy(value).into_owned())
                .filter(|value| !value.is_empty())
        };
        match discriminator {
            // The listener terminates TLS before a startup packet exists, and
            // nothing carries the negotiated server name this far yet.
            TenantDiscriminator::Sni => None,
            TenantDiscriminator::StartupOptions => {
                option_value(text(b"options").as_deref()?, &self.option_key)
            }
            TenantDiscriminator::DatabaseName => text(b"database"),
            TenantDiscriminator::Role => (!role.is_empty()).then(|| role.to_owned()),
        }
    }
}

/// Reads one `-c name=value` out of a startup packet's `options` string.
///
/// `options` is split on unescaped whitespace exactly as `PostgreSQL` splits it,
/// so a value containing a space is reachable by escaping it and a backslash is
/// reachable by doubling it. Anything that is not `-c name=value` is skipped
/// rather than rejected: the string belongs to the backend, and the proxy is
/// only reading one key out of it.
fn option_value(options: &str, key: &str) -> Option<String> {
    let mut words = Vec::new();
    let mut word = String::new();
    let mut escaped = false;
    for character in options.chars() {
        match character {
            '\\' if !escaped => escaped = true,
            character if character.is_whitespace() && !escaped => {
                if !word.is_empty() {
                    words.push(std::mem::take(&mut word));
                }
            }
            character => {
                word.push(character);
                escaped = false;
            }
        }
    }
    if !word.is_empty() {
        words.push(word);
    }

    let mut pending = false;
    for word in words {
        let assignment = if pending {
            pending = false;
            Some(word.as_str())
        } else if word == "-c" {
            pending = true;
            None
        } else {
            word.strip_prefix("-c").filter(|rest| !rest.is_empty())
        };
        if let Some((name, value)) = assignment.and_then(|text| text.split_once('='))
            && name == key
            && !value.is_empty()
        {
            return Some(value.to_owned());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use pgelastic_wire::ProtocolVersion;

    fn startup(parameters: &[(&str, &str)]) -> StartupMessage {
        StartupMessage::new(
            ProtocolVersion::V3_0,
            parameters
                .iter()
                .map(|(name, value)| {
                    (
                        Bytes::copy_from_slice(name.as_bytes()),
                        Bytes::copy_from_slice(value.as_bytes()),
                    )
                })
                .collect(),
        )
    }

    fn resolver(discriminators: &[TenantDiscriminator]) -> TenantResolver {
        TenantResolver::new(&RoutingConfig {
            tenant_discriminators: discriminators.to_vec(),
            ..RoutingConfig::default()
        })
    }

    #[test]
    fn the_default_single_tenant_shape_is_the_login_role() {
        let resolver = resolver(&[TenantDiscriminator::Role]);
        let tenant = resolver
            .resolve(&startup(&[("database", "orders")]), "acme")
            .unwrap();
        assert_eq!(tenant, "acme");
    }

    #[test]
    fn a_pool_behind_one_service_reads_the_tenant_off_the_database_name() {
        let resolver = resolver(&[TenantDiscriminator::DatabaseName]);
        let tenant = resolver
            .resolve(&startup(&[("database", "orders")]), "app_user")
            .unwrap();
        assert_eq!(tenant, "orders");
    }

    #[test]
    fn two_clients_of_two_tenants_resolve_to_two_tenants() {
        let resolver = resolver(&[TenantDiscriminator::DatabaseName]);
        let one = resolver
            .resolve(&startup(&[("database", "test")]), "app_user")
            .unwrap();
        let two = resolver
            .resolve(&startup(&[("database", "test2")]), "app_user")
            .unwrap();
        assert_ne!(one, two);
    }

    #[test]
    fn a_startup_option_names_the_tenant_when_the_database_does_not() {
        let resolver = resolver(&[
            TenantDiscriminator::StartupOptions,
            TenantDiscriminator::DatabaseName,
        ]);
        let tenant = resolver
            .resolve(
                &startup(&[("options", "-c pgelastic.tenant=acme"), ("database", "acme")]),
                "app_user",
            )
            .unwrap();
        assert_eq!(tenant, "acme");
    }

    #[test]
    fn discriminators_that_disagree_refuse_the_connection_rather_than_guessing() {
        let resolver = resolver(&[
            TenantDiscriminator::StartupOptions,
            TenantDiscriminator::DatabaseName,
        ]);
        let error = resolver
            .resolve(
                &startup(&[
                    ("options", "-c pgelastic.tenant=acme"),
                    ("database", "globex"),
                ]),
                "app_user",
            )
            .unwrap_err();
        assert!(error.to_string().contains("ambiguous"), "{error}");
    }

    #[test]
    fn sni_contributes_nothing_and_lets_the_rest_of_the_list_decide() {
        let resolver = resolver(&[
            TenantDiscriminator::Sni,
            TenantDiscriminator::DatabaseName,
        ]);
        let tenant = resolver
            .resolve(&startup(&[("database", "orders")]), "app_user")
            .unwrap();
        assert_eq!(tenant, "orders");
    }

    #[test]
    fn a_connection_naming_no_database_falls_back_to_the_role_postgres_would_have_used() {
        let resolver = resolver(&[TenantDiscriminator::DatabaseName]);
        let tenant = resolver.resolve(&startup(&[]), "acme").unwrap();
        assert_eq!(tenant, "acme");
    }

    #[test]
    fn the_option_key_is_read_in_both_spellings_postgres_accepts() {
        assert_eq!(
            option_value("-c pgelastic.tenant=acme", "pgelastic.tenant"),
            Some("acme".to_owned())
        );
        assert_eq!(
            option_value("-cpgelastic.tenant=acme", "pgelastic.tenant"),
            Some("acme".to_owned())
        );
    }

    #[test]
    fn an_escaped_space_stays_inside_its_value() {
        assert_eq!(
            option_value(r"-c pgelastic.tenant=two\ words", "pgelastic.tenant"),
            Some("two words".to_owned())
        );
    }

    #[test]
    fn unrelated_options_are_stepped_over_rather_than_refused() {
        assert_eq!(
            option_value(
                "-c search_path=public -c pgelastic.tenant=acme --foo",
                "pgelastic.tenant"
            ),
            Some("acme".to_owned())
        );
        assert_eq!(option_value("-c search_path=public", "pgelastic.tenant"), None);
    }

    #[test]
    fn an_empty_value_is_not_a_tenant_name() {
        assert_eq!(option_value("-c pgelastic.tenant=", "pgelastic.tenant"), None);
    }
}
