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

package autoscale

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

// cronParser accepts the standard five-field expression the API documents, and nothing
// else. Descriptors such as @daily and the optional seconds field are deliberately not
// enabled: a window that means something different from what a crontab would do is a
// window somebody will misread during a change freeze.
var cronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

// WindowOpen reports whether now falls inside any of the given recurring windows.
//
// A window is a cron activation plus a duration, so "is it open" is the question "was there
// an activation in the half-open interval (now-duration, now]". robfig's schedules only walk
// forwards, which is exactly enough: the first activation strictly after now-duration either
// falls at or before now, in which case its window is still open, or it does not.
//
// An expression that does not parse opens nothing and says so. Silently treating a
// malformed blackout window as "no blackout" is how an autoscaler acts during a freeze.
func WindowOpen(now time.Time, windows []pgelasticv1alpha1.TimeWindow) (bool, error) {
	for _, window := range windows {
		open, err := oneWindowOpen(now, window)
		if err != nil {
			return false, err
		}
		if open {
			return true, nil
		}
	}
	return false, nil
}

func oneWindowOpen(now time.Time, window pgelasticv1alpha1.TimeWindow) (bool, error) {
	schedule, err := cronParser.Parse(window.Schedule)
	if err != nil {
		return false, fmt.Errorf("parsing schedule %q: %w", window.Schedule, err)
	}
	location := time.UTC
	if window.TimeZone != "" {
		location, err = time.LoadLocation(window.TimeZone)
		if err != nil {
			return false, fmt.Errorf("loading time zone %q: %w", window.TimeZone, err)
		}
	}
	if window.Duration.Duration <= 0 {
		return false, fmt.Errorf("window %q has a non-positive duration", window.Schedule)
	}

	local := now.In(location)
	activation := schedule.Next(local.Add(-window.Duration.Duration))
	return !activation.After(local), nil
}

// NextWindow reports when the next of these windows opens, for a message that tells an
// operator when the thing they are waiting for becomes possible.
func NextWindow(now time.Time, windows []pgelasticv1alpha1.TimeWindow) (time.Time, bool) {
	next := time.Time{}
	for _, window := range windows {
		schedule, err := cronParser.Parse(window.Schedule)
		if err != nil {
			continue
		}
		location := time.UTC
		if window.TimeZone != "" {
			if loaded, err := time.LoadLocation(window.TimeZone); err == nil {
				location = loaded
			}
		}
		activation := schedule.Next(now.In(location))
		if next.IsZero() || activation.Before(next) {
			next = activation
		}
	}
	return next, !next.IsZero()
}
