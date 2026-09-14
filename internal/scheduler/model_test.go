package scheduler

import "testing"

func TestResources(t *testing.T) {
	for _, tt := range []struct {
		r, cap Resources
		want   bool
	}{{Resources{1, 512, 0}, Resources{2, 1024, 0}, true}, {Resources{1, 512, 1}, Resources{2, 1024, 0}, false}, {Resources{-1, 0, 0}, Resources{1, 1, 1}, false}, {Resources{2, 1025, 0}, Resources{2, 1024, 0}, false}} {
		if got := tt.r.Fits(tt.cap); got != tt.want {
			t.Errorf("%v fits %v = %v", tt.r, tt.cap, got)
		}
	}
}
func TestRetryPolicy(t *testing.T) {
	if RetryState(1, 3) != "QUEUED" || RetryState(3, 3) != "FAILED" {
		t.Fatal("retry budget")
	}
}
func TestValidate(t *testing.T) {
	j := Job{Name: "test", Command: []string{"true"}, Resources: Resources{1, 64, 0}, MaxAttempts: 1, TimeoutSeconds: 10, IdempotencyKey: "test"}
	if e := j.Validate(); e != nil {
		t.Fatal(e)
	}
	j.Environment = map[string]string{"RUNGRID_JOB_ID": "forged"}
	if j.Validate() == nil {
		t.Fatal("reserved environment accepted")
	}
}
func FuzzResourceFits(f *testing.F) {
	f.Add(1, 2, 3, 4, 5, 6)
	f.Fuzz(func(t *testing.T, a, b, c, x, y, z int) {
		r := Resources{a, b, c}
		cap := Resources{x, y, z}
		if r.Fits(cap) && (!r.Valid() || a > x || b > y || c > z) {
			t.Fatal("resource invariant")
		}
	})
}
