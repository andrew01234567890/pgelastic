/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package scram derives the two halves of a SCRAM-SHA-256 credential from one password.
//
// They are different things and the difference is the point of SCRAM. The verifier is what a
// server stores: enough to check a client, never enough to impersonate one. SaltedPassword is
// what a client needs: it can answer a challenge, so it is the half that must not be stored
// where a verifier would do.
//
// pgelastic needs both, on opposite legs of the same connection. PostgreSQL stores the verifier
// for the tenant's role; the proxy holds SaltedPassword because on the backend leg it is the
// SCRAM *client*. Deriving them together from one password is what keeps them agreeing - the
// salt and iteration count are part of the credential, not a tunable, so a verifier computed
// with different ones authenticates nobody.
package scram

import (
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
)

// SaltLength is PostgreSQL's own default for a stored verifier.
const SaltLength = 16

// Credential is one password expressed as the two halves a SCRAM exchange needs.
type Credential struct {
	// Verifier is the literal PostgreSQL accepts in ALTER ROLE ... PASSWORD, in the form
	// SCRAM-SHA-256$<iterations>:<salt>$<StoredKey>:<ServerKey>. Passing this rather than the
	// password means the plaintext never crosses the wire to the database, and the same bytes
	// land on every instance the tenant is ever provisioned on.
	Verifier string
	// SaltedPassword is what the proxy proves the credential with on the backend leg, base64
	// encoded. ClientKey and ServerKey both derive from it.
	SaltedPassword string
	// Salt and Iterations are carried alongside because SaltedPassword means nothing without
	// the parameters it was derived under.
	Salt       string
	Iterations int32
}

// Derive computes both halves of a credential from a password.
func Derive(password string, iterations int32) (Credential, error) {
	if iterations <= 0 {
		return Credential{}, fmt.Errorf("scram iterations must be positive, got %d", iterations)
	}
	salt := make([]byte, SaltLength)
	if _, err := rand.Read(salt); err != nil {
		return Credential{}, fmt.Errorf("generating a salt: %w", err)
	}
	return deriveWithSalt(password, salt, iterations)
}

func deriveWithSalt(password string, salt []byte, iterations int32) (Credential, error) {
	salted, err := pbkdf2.Key(sha256.New, password, salt, int(iterations), sha256.Size)
	if err != nil {
		return Credential{}, fmt.Errorf("deriving the salted password: %w", err)
	}
	clientKey := mac(salted, "Client Key")
	storedKey := sha256.Sum256(clientKey)
	serverKey := mac(salted, "Server Key")

	encode := base64.StdEncoding.EncodeToString
	return Credential{
		Verifier: fmt.Sprintf("SCRAM-SHA-256$%d:%s$%s:%s",
			iterations, encode(salt), encode(storedKey[:]), encode(serverKey)),
		SaltedPassword: encode(salted),
		Salt:           encode(salt),
		Iterations:     iterations,
	}, nil
}

func mac(key []byte, message string) []byte {
	writer := hmac.New(sha256.New, key)
	writer.Write([]byte(message))
	return writer.Sum(nil)
}
