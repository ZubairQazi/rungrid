package scheduler

import (
	"crypto/rand"
	"fmt"
	"regexp"
	"strings"
)

type Resources struct {
	CPU      int `json:"cpu"`
	MemoryMB int `json:"memory_mb"`
	GPU      int `json:"gpu"`
}

func (r Resources) Valid() bool {
	return r.CPU >= 0 && r.MemoryMB >= 0 && r.GPU >= 0 && r.CPU <= 1000000 && r.MemoryMB <= 1000000000 && r.GPU <= 1000000
}
func (r Resources) Fits(cap Resources) bool {
	return r.Valid() && r.CPU <= cap.CPU && r.MemoryMB <= cap.MemoryMB && r.GPU <= cap.GPU
}

type Job struct {
	Name           string            `json:"name"`
	Command        []string          `json:"command"`
	Environment    map[string]string `json:"environment"`
	Resources      Resources         `json:"resources"`
	MaxAttempts    int               `json:"max_attempts"`
	TimeoutSeconds int               `json:"timeout_seconds"`
	IdempotencyKey string            `json:"idempotency_key"`
	ArtifactPrefix string            `json:"artifact_prefix"`
}

var envKey = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func (j Job) Validate() error {
	if len(j.Name) == 0 || len(j.Name) > 256 || len(j.IdempotencyKey) == 0 || len(j.IdempotencyKey) > 256 {
		return fmt.Errorf("name and idempotency_key must contain 1..256 bytes")
	}
	if len(j.Command) == 0 || j.Command[0] == "" || len(j.Command) > 1024 {
		return fmt.Errorf("command is required")
	}
	for _, a := range j.Command {
		if strings.ContainsRune(a, 0) {
			return fmt.Errorf("command contains NUL")
		}
	}
	if !j.Resources.Valid() || j.Resources.CPU < 1 || j.Resources.MemoryMB < 1 {
		return fmt.Errorf("positive cpu and memory_mb, nonnegative gpu required")
	}
	if j.MaxAttempts < 1 || j.MaxAttempts > 100 || j.TimeoutSeconds < 1 || j.TimeoutSeconds > 604800 {
		return fmt.Errorf("max_attempts must be 1..100 and timeout_seconds 1..604800")
	}
	for k, v := range j.Environment {
		if !envKey.MatchString(k) || strings.ContainsRune(v, 0) || strings.HasPrefix(k, "RUNGRID_") {
			return fmt.Errorf("invalid or reserved environment variable %q", k)
		}
	}
	if strings.HasPrefix(j.ArtifactPrefix, "/") || strings.Contains(j.ArtifactPrefix, "..") || len(j.ArtifactPrefix) > 512 {
		return fmt.Errorf("invalid artifact_prefix")
	}
	return nil
}
func ID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:])
}
func RetryState(number, max int) string {
	if number < max {
		return "QUEUED"
	}
	return "FAILED"
}
