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

package pgversion_test

import (
	"testing"

	"github.com/andrew01234567890/pgelastic/internal/pgversion"
)

func TestANumberReadsAsTheVersionPeopleSay(t *testing.T) {
	for _, testCase := range []struct {
		num   int
		major int
		text  string
	}{
		{num: 180000, major: 18, text: "18.0"},
		{num: 180004, major: 18, text: "18.4"},
		// A beta reports the major with no minor release, which is exactly how it should
		// order against 18.4: later.
		{num: 190000, major: 19, text: "19.0"},
		{num: 200013, major: 20, text: "20.13"},
	} {
		version, err := pgversion.FromNum(testCase.num)
		if err != nil {
			t.Fatalf("FromNum(%d): %v", testCase.num, err)
		}
		if version.Major() != testCase.major {
			t.Errorf("FromNum(%d).Major() = %d, want %d", testCase.num, version.Major(), testCase.major)
		}
		if version.String() != testCase.text {
			t.Errorf("FromNum(%d) reads as %q, want %q", testCase.num, version.String(), testCase.text)
		}
		if version.Num() != testCase.num {
			t.Errorf("FromNum(%d).Num() = %d", testCase.num, version.Num())
		}
	}
}

// 9.6.5 is 90605 under the older scheme, so reading it as major/10000 would call it
// PostgreSQL 9 with minor release 5 - a number that is wrong in a way no range would catch.
func TestAPreTenNumberIsRefusedRatherThanMisread(t *testing.T) {
	if _, err := pgversion.FromNum(90605); err == nil {
		t.Error("server_version_num 90605 was accepted; it is 9.6.5 under a different scheme")
	}
}

// The two constraints this tree will actually write: "18 or later" for anything that reads a
// catalog 18 introduced, and "18 only" for a value 19 changes out from under.
const (
	eighteenOrLater = ">= 18"
	eighteenOnly    = ">= 18, < 19"
)

func TestARangeSaysWhichMajorsApply(t *testing.T) {
	for _, testCase := range []struct {
		constraint string
		num        int
		want       bool
	}{
		{constraint: eighteenOrLater, num: 180004, want: true},
		{constraint: eighteenOrLater, num: 170009, want: false},
		{constraint: eighteenOrLater, num: 190000, want: true},
		{constraint: "< 19", num: 180004, want: true},
		{constraint: "< 19", num: 190000, want: false},
		{constraint: eighteenOnly, num: 180000, want: true},
		{constraint: eighteenOnly, num: 190000, want: false},
		{constraint: eighteenOnly, num: 170000, want: false},
		// The one that would catch a mapping mistake: a late minor release of the major
		// below the floor must stay out, however high its minor number is.
		{constraint: eighteenOrLater, num: 179999, want: false},
	} {
		parsed, err := pgversion.ParseRange(testCase.constraint)
		if err != nil {
			t.Fatalf("ParseRange(%q): %v", testCase.constraint, err)
		}
		version, err := pgversion.FromNum(testCase.num)
		if err != nil {
			t.Fatalf("FromNum(%d): %v", testCase.num, err)
		}
		if got := parsed.Includes(version); got != testCase.want {
			t.Errorf("%q includes %s = %v, want %v", testCase.constraint, version, got, testCase.want)
		}
	}
}

// The zero Range is what an entry with nothing to say about versions carries, and it has to
// mean "everywhere". If it meant "nowhere", adding the gate to the tree would silently switch
// off every query and every parameter that did not opt in.
func TestAnUnconstrainedRangeIncludesEverything(t *testing.T) {
	var everywhere pgversion.Range
	if !everywhere.Unconstrained() {
		t.Error("the zero Range does not report itself unconstrained")
	}
	for _, num := range []int{100000, 180004, 190000} {
		version, err := pgversion.FromNum(num)
		if err != nil {
			t.Fatalf("FromNum(%d): %v", num, err)
		}
		if !everywhere.Includes(version) {
			t.Errorf("the zero Range excludes %s", version)
		}
	}
	if parsed, err := pgversion.ParseRange(""); err != nil || !parsed.Unconstrained() {
		t.Errorf("an empty constraint is not the unconstrained range: %v, %v", parsed, err)
	}
}

func TestAMalformedRangeIsRefusedAtParse(t *testing.T) {
	if _, err := pgversion.ParseRange("eighteen or later"); err == nil {
		t.Error("a constraint that is not a constraint was accepted")
	}
}
