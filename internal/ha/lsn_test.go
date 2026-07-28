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

package ha

import "testing"

func TestParseLSN(t *testing.T) {
	for value, want := range map[string]LSN{
		"":            0,
		"0/0":         0,
		"0/16B3748":   0x16B3748,
		"1/00000000":  1 << 32,
		"10/00000000": 0x10 << 32,
		"9/FFFFFFFF":  0x9FFFFFFFF,
	} {
		got, err := ParseLSN(value)
		if err != nil {
			t.Fatalf("ParseLSN(%q): %v", value, err)
		}
		if got != want {
			t.Fatalf("ParseLSN(%q) = %d, want %d", value, got, want)
		}
	}
}

func TestParseLSNRejectsNonsense(t *testing.T) {
	for _, value := range []string{"nonsense", "1/", "/1", "zz/1", "1/zz"} {
		if _, err := ParseLSN(value); err == nil {
			t.Fatalf("ParseLSN(%q) accepted an unreadable position", value)
		}
	}
}

func TestAnUnreadableLSNRanksLastRatherThanFailingTheDecision(t *testing.T) {
	if got := MustParseLSN("nonsense"); got != 0 {
		t.Fatalf("an unreadable position must rank behind every known one, got %d", got)
	}
}

func TestLSNRoundTripsThroughPostgresForm(t *testing.T) {
	const value = "2/16B3748"
	parsed, err := ParseLSN(value)
	if err != nil {
		t.Fatal(err)
	}
	if got := parsed.String(); got != "2/016B3748" {
		t.Fatalf("String() = %q", got)
	}
	again, err := ParseLSN(parsed.String())
	if err != nil || again != parsed {
		t.Fatalf("re-parsing %q gave %d, %v", parsed.String(), again, err)
	}
}
