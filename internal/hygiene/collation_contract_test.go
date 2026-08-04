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

package hygiene

import (
	"strings"
	"testing"
)

// data_checksums is a bool on PostgreSQL 18 and a four-value enum on 19 - on, off,
// inprogress-on, inprogress-off - because 19 can enable and disable checksums on a running
// cluster. Casting it to boolean works for the first two and raises 22P02 for either
// inprogress value.
//
// That would not fail narrowly. It is one column of the collation contract, which also carries
// the system identifier, the WAL segment size and the locale tuple that the migration
// preflight and the pool-join gate both read - so the cast failing makes a tenant unmovable,
// and the reason is a cast nobody was looking at.
func TestTheCollationContractSurvivesAnEnumDataChecksums(t *testing.T) {
	source := read(t, "internal/instance/agent/postgres.go")

	if strings.Contains(source, "current_setting('data_checksums')::boolean") {
		t.Error("data_checksums is cast to boolean, which raises 22P02 on a PostgreSQL 19 " +
			"cluster part-way through enabling or disabling checksums - and takes the whole " +
			"collation contract with it")
	}
	for _, want := range []string{"'on'", "'inprogress-on'"} {
		if !strings.Contains(source, want) {
			t.Errorf("the contract does not count %s as checksums being on", want)
		}
	}
}
