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
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

func windowAt(schedule string, duration time.Duration, zone string) pgelasticv1alpha1.TimeWindow {
	return pgelasticv1alpha1.TimeWindow{
		Schedule: schedule,
		Duration: metav1.Duration{Duration: duration},
		TimeZone: zone,
	}
}

func TestWindowOpenTracksTheCronActivationAndItsDuration(t *testing.T) {
	// Weekdays at 08:00 UTC for ten hours: the design's business-hours blackout.
	windows := []pgelasticv1alpha1.TimeWindow{windowAt("0 8 * * 1-5", 10*time.Hour, "UTC")}

	for _, testCase := range []struct {
		name string
		at   time.Time
		want bool
	}{
		{"just before the window opens", time.Date(2026, 7, 27, 7, 59, 0, 0, time.UTC), false},
		{"at the moment it opens", time.Date(2026, 7, 27, 8, 0, 0, 0, time.UTC), true},
		{"in the middle", time.Date(2026, 7, 27, 13, 0, 0, 0, time.UTC), true},
		{"one second before it closes", time.Date(2026, 7, 27, 17, 59, 59, 0, time.UTC), true},
		{"after it closes", time.Date(2026, 7, 27, 18, 30, 0, 0, time.UTC), false},
		{"the same hour on a Sunday", time.Date(2026, 7, 26, 13, 0, 0, 0, time.UTC), false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			open, err := WindowOpen(testCase.at, windows)
			if err != nil {
				t.Fatalf("WindowOpen: %v", err)
			}
			if open != testCase.want {
				t.Errorf("open = %v at %s, want %v", open, testCase.at, testCase.want)
			}
		})
	}
}

func TestWindowIsEvaluatedInItsOwnTimeZone(t *testing.T) {
	windows := []pgelasticv1alpha1.TimeWindow{windowAt("0 8 * * 1-5", 10*time.Hour, "Europe/London")}

	// 08:30 London in July is 07:30 UTC: inside the window in London terms, outside it if the
	// zone were ignored.
	at := time.Date(2026, 7, 27, 7, 30, 0, 0, time.UTC)
	open, err := WindowOpen(at, windows)
	if err != nil {
		t.Fatalf("WindowOpen: %v", err)
	}
	if !open {
		t.Error("a window declared in Europe/London was evaluated in UTC")
	}
}

func TestNoWindowsIsNoWindowOpen(t *testing.T) {
	open, err := WindowOpen(time.Now(), nil)
	if err != nil {
		t.Fatalf("WindowOpen: %v", err)
	}
	if open {
		t.Error("an empty window list reports a window open")
	}
}

// A malformed blackout window must not read as "no blackout". That failure would let the
// autoscaler act during a change freeze somebody believed they had declared.
func TestAMalformedScheduleIsAnErrorAndNotAnOpenGate(t *testing.T) {
	_, err := WindowOpen(time.Now(), []pgelasticv1alpha1.TimeWindow{windowAt("not a cron expression", time.Hour, "UTC")})
	if err == nil {
		t.Fatal("a malformed cron expression was accepted")
	}

	_, err = WindowOpen(time.Now(), []pgelasticv1alpha1.TimeWindow{windowAt("0 8 * * *", 0, "UTC")})
	if err == nil {
		t.Fatal("a window with a zero duration was accepted")
	}

	_, err = WindowOpen(time.Now(), []pgelasticv1alpha1.TimeWindow{windowAt("0 8 * * *", time.Hour, "Mars/Olympus_Mons")})
	if err == nil {
		t.Fatal("an unknown time zone was accepted")
	}
}

func TestNextWindowNamesWhenTheGateReopens(t *testing.T) {
	windows := []pgelasticv1alpha1.TimeWindow{windowAt("0 2 * * *", time.Hour, "UTC")}
	at := time.Date(2026, 7, 27, 13, 0, 0, 0, time.UTC)

	next, ok := NextWindow(at, windows)
	if !ok {
		t.Fatal("no next window for a daily schedule")
	}
	want := time.Date(2026, 7, 28, 2, 0, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Errorf("next window at %s, want %s", next, want)
	}
}
