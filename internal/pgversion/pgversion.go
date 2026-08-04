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

// Package pgversion says which PostgreSQL majors a thing applies to.
//
// There are two consumers and they were going to grow one of these each. A metric query has to
// name the majors whose catalog carries its columns - pg_stat_wal lost four in 18 and
// pg_stat_subscription_stats renamed one in 19 - and a computed parameter has to name the
// majors its value is right for, because PostgreSQL 19 doubles max_locks_per_transaction's
// default and says in its own release note that "settings must now be doubled to match their
// capacity in previous releases". Same question, same answer, one package.
//
// The version is read from server_version_num rather than parsed out of version(). During a
// beta cycle version() carries a beta suffix and the rest of the time it carries whatever the
// distribution built it with; neither is orderable, and PostgreSQL 19 is in beta right now.
package pgversion

import (
	"fmt"

	"github.com/Masterminds/semver/v3"
)

// oldestNumbering is the first major that numbered itself major*10000 + minor. Before 10 the
// scheme was major*10000 + minor*100 + patch, so 90605 is 9.6.5 - a different shape that this
// tree has never had to read and would be wrong to guess at.
const oldestNumbering = 100000

// Version is one server's version, as server_version_num reports it.
type Version struct {
	num      int
	semantic semver.Version
}

// FromNum reads a server_version_num.
//
// The mapping is PostgreSQL's own: major is num/10000 and the minor release is num%100, with
// nothing in between since 10. That lands in semver as major.0.patch, which is what makes a
// constraint written the way an operator thinks about it - ">= 18", "< 19" - mean what it
// looks like it means.
func FromNum(num int) (Version, error) {
	if num < oldestNumbering {
		return Version{}, fmt.Errorf(
			"server_version_num %d is PostgreSQL 9.x or older, which numbered its versions "+
				"differently and this tree has never supported", num)
	}
	return Version{
		num:      num,
		semantic: *semver.New(uint64(num/10000), 0, uint64(num%100), "", ""),
	}, nil
}

// Major is the number people mean when they say "PostgreSQL 18".
func (v Version) Major() int { return v.num / 10000 }

// Num is the server_version_num it was read from.
func (v Version) Num() int { return v.num }

func (v Version) String() string { return fmt.Sprintf("%d.%d", v.Major(), v.num%100) }

// Range is the set of majors something applies to, written as a semver constraint.
//
// The zero Range includes every version. That is the right default and not an oversight: a
// query or a parameter with no version gate is one that has always been correct everywhere,
// and making the common case require a constraint would mean every entry carried one whether
// or not it had anything to say.
type Range struct {
	constraint *semver.Constraints
	text       string
}

// ParseRange reads a constraint such as ">= 18", "< 19" or ">= 18, < 19".
func ParseRange(text string) (Range, error) {
	if text == "" {
		return Range{}, nil
	}
	constraint, err := semver.NewConstraint(text)
	if err != nil {
		return Range{}, fmt.Errorf("version range %q: %w", text, err)
	}
	return Range{constraint: constraint, text: text}, nil
}

// MustParseRange is ParseRange for a constraint written in this repository's own source, where
// a malformed one is a programming error rather than input.
func MustParseRange(text string) Range {
	parsed, err := ParseRange(text)
	if err != nil {
		panic(err)
	}
	return parsed
}

// Includes reports whether the version is in the range.
func (r Range) Includes(v Version) bool {
	if r.constraint == nil {
		return true
	}
	return r.constraint.Check(&v.semantic)
}

// Unconstrained reports a range that includes everything, which is how a caller tells "runs
// anywhere" from "runs on the majors this names" without comparing strings.
func (r Range) Unconstrained() bool { return r.constraint == nil }

func (r Range) String() string {
	if r.constraint == nil {
		return "every version"
	}
	return r.text
}
