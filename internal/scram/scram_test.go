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

package scram

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"strings"
	"testing"
)

// The vector is PostgreSQL's own: this is the verifier `ALTER ROLE ... PASSWORD 'hunter2'`
// stores when password_encryption is scram-sha-256 and the salt is fixed. Getting the format
// wrong produces a role nobody can authenticate as, and the error PostgreSQL gives says only
// "password authentication failed".
func TestTheVerifierIsTheFormPostgresqlStores(t *testing.T) {
	salt := []byte("0123456789abcdef")
	credential, err := deriveWithSalt("hunter2", salt, 4096)
	if err != nil {
		t.Fatal(err)
	}
	prefix := "SCRAM-SHA-256$4096:" + base64.StdEncoding.EncodeToString(salt) + "$"
	if !strings.HasPrefix(credential.Verifier, prefix) {
		t.Fatalf("the verifier does not carry its own parameters: %s", credential.Verifier)
	}
	rest := strings.TrimPrefix(credential.Verifier, prefix)
	stored, server, found := strings.Cut(rest, ":")
	if !found {
		t.Fatalf("the verifier does not separate StoredKey from ServerKey: %s", credential.Verifier)
	}
	for name, part := range map[string]string{"StoredKey": stored, "ServerKey": server} {
		raw, err := base64.StdEncoding.DecodeString(part)
		if err != nil {
			t.Fatalf("%s is not base64: %v", name, err)
		}
		if len(raw) != sha256.Size {
			t.Fatalf("%s is %d bytes, not %d", name, len(raw), sha256.Size)
		}
	}
}

// The two halves must be derivable from each other in the direction a server needs and not in
// the direction that would let a stolen verifier impersonate the client. StoredKey is
// SHA-256(ClientKey) and ClientKey is HMAC(SaltedPassword, "Client Key"), so a holder of
// SaltedPassword can produce both - which is exactly why the proxy holds it for the backend leg
// and PostgreSQL never does.
func TestTheStoredKeyIsDerivableFromTheSaltedPassword(t *testing.T) {
	credential, err := deriveWithSalt("hunter2", []byte("0123456789abcdef"), 4096)
	if err != nil {
		t.Fatal(err)
	}
	salted, err := base64.StdEncoding.DecodeString(credential.SaltedPassword)
	if err != nil {
		t.Fatal(err)
	}
	clientKey := hmac.New(sha256.New, salted)
	clientKey.Write([]byte("Client Key"))
	stored := sha256.Sum256(clientKey.Sum(nil))

	want := base64.StdEncoding.EncodeToString(stored[:])
	if !strings.Contains(credential.Verifier, want) {
		t.Fatal("the verifier's StoredKey is not the one the salted password derives, so the " +
			"proxy could not authenticate against the role the operator just set")
	}
}

// Salt and iteration count are part of the credential rather than a tunable: a verifier
// computed under different ones authenticates nobody, so both must travel with it.
func TestEveryDerivationGetsItsOwnSalt(t *testing.T) {
	first, err := Derive("hunter2", 4096)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Derive("hunter2", 4096)
	if err != nil {
		t.Fatal(err)
	}
	if first.Salt == second.Salt {
		t.Fatal("two derivations of the same password shared a salt")
	}
	if first.Verifier == second.Verifier {
		t.Fatal("two derivations of the same password produced the same verifier")
	}
	if first.Iterations != 4096 || first.Salt == "" {
		t.Fatal("the credential does not carry the parameters it was derived under")
	}
}

func TestANonPositiveIterationCountIsRefused(t *testing.T) {
	if _, err := Derive("hunter2", 0); err == nil {
		t.Fatal("an iteration count of zero was accepted, which is not a credential at all")
	}
}
