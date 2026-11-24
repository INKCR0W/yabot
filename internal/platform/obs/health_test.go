package obs

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"
)

type malformedHealthError struct{}

func (malformedHealthError) Error() string {
	panic("cannot format error")
}

func TestHealthCheck(t *testing.T) {
	type registration struct {
		name  string
		check func() error
	}
	cases := []struct {
		name   string
		checks []registration
		want   Report
	}{
		{
			name: "empty",
			want: Report{Healthy: true},
		},
		{
			name:   "healthy",
			checks: []registration{{"store", func() error { return nil }}},
			want:   Report{Healthy: true},
		},
		{
			name:   "one failure",
			checks: []registration{{"store", func() error { return errors.New("unavailable") }}},
			want:   Report{Failures: []Failure{{Name: "store", Error: "unavailable"}}},
		},
		{
			name:   "failure with empty message",
			checks: []registration{{"store", func() error { return errors.New("") }}},
			want:   Report{Failures: []Failure{{Name: "store"}}},
		},
		{
			name:   "panic while formatting error",
			checks: []registration{{"store", func() error { return malformedHealthError{} }}},
			want:   Report{Failures: []Failure{{Name: "store", Error: "check panicked: cannot format error"}}},
		},
		{
			name: "sorted failures after panic",
			checks: []registration{
				{"store", func() error { return errors.New("unavailable") }},
				{"dispatch", func() error { return nil }},
				{"ingress", func() error { panic("unexpected") }},
				{"sender", func() error { return errors.New("stalled") }},
			},
			want: Report{Failures: []Failure{
				{Name: "ingress", Error: "check panicked: unexpected"},
				{Name: "sender", Error: "stalled"},
				{Name: "store", Error: "unavailable"},
			}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var health Health
			for _, entry := range tc.checks {
				health.Register(entry.name, entry.check)
			}
			got := health.Check()
			if got.Healthy != tc.want.Healthy || !slices.Equal(got.Failures, tc.want.Failures) {
				t.Fatalf("Check() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestHealthRegisterPanics(t *testing.T) {
	check := func() error { return nil }
	cases := []struct {
		name        string
		registerOld bool
		newCheck    func() error
	}{
		{name: "duplicate name", registerOld: true, newCheck: check},
		{name: "nil check"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			health := NewHealth()
			if tc.registerOld {
				health.Register("check", check)
			}
			defer func() {
				if recover() == nil {
					t.Error("Register did not panic")
				}
			}()
			health.Register("check", tc.newCheck)
		})
	}
}

func TestHealthRegisterWhileChecking(t *testing.T) {
	health := NewHealth()
	started := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	var firstCheck sync.Once
	health.Register("existing", func() error {
		firstCheck.Do(func() {
			close(started)
			<-release
		})
		return nil
	})

	report := make(chan Report, 1)
	go func() { report <- health.Check() }()
	<-started

	registered := make(chan struct{})
	go func() {
		health.Register("new", func() error { return errors.New("unavailable") })
		close(registered)
	}()
	select {
	case <-registered:
	case <-time.After(5 * time.Second):
		t.Fatal("Register blocked while a health check was running")
	}
	release <- struct{}{}

	if got := <-report; !got.Healthy || len(got.Failures) != 0 {
		t.Fatalf("in-flight Check() = %+v, want healthy", got)
	}
	want := []Failure{{Name: "new", Error: "unavailable"}}
	if got := health.Check(); got.Healthy || !slices.Equal(got.Failures, want) {
		t.Fatalf("next Check() = %+v, want failures %+v", got, want)
	}
}

func TestHealthConcurrentRegisterAndCheck(t *testing.T) {
	const workerCount = 4
	const checksPerWorker = 32
	var health Health
	checkErr := errors.New("unavailable")
	start := make(chan struct{})
	var workers sync.WaitGroup
	for worker := range workerCount {
		workers.Go(func() {
			<-start
			for index := range checksPerWorker {
				name := fmt.Sprintf("check-%d-%d", worker, index)
				health.Register(name, func() error { return checkErr })
				report := health.Check()
				want := Failure{Name: name, Error: checkErr.Error()}
				if report.Healthy || !slices.Contains(report.Failures, want) {
					t.Errorf("Check() is missing the registered failure %+v", want)
					return
				}
			}
		})
	}
	close(start)
	workers.Wait()

	if got, want := len(health.Check().Failures), workerCount*checksPerWorker; got != want {
		t.Errorf("Check() reported %d failures, want %d", got, want)
	}
}
