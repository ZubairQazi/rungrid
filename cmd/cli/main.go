package main

import (
	"bytes"
	"fmt"
	"github.com/ZubairQazi/rungrid/internal/scheduler"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

func run() error {
	args := os.Args[1:]
	if len(args) == 0 {
		return fmt.Errorf("usage: rungrid submit|batch FILE; list; get|cancel|retry|attempts|events|artifacts ID; workers")
	}
	base := os.Getenv("RUNGRID_URL")
	if base == "" {
		base = "http://localhost:8080"
	}
	path := "/v1/jobs"
	method := "GET"
	var body []byte
	var e error
	switch args[0] {
	case "submit", "batch":
		if len(args) != 2 {
			return fmt.Errorf("file required")
		}
		body, e = os.ReadFile(args[1])
		if e != nil {
			return e
		}
		method = "POST"
		if args[0] == "batch" {
			path += "/batch"
		}
	case "list":
	case "workers":
		path = "/v1/workers"
	case "get", "cancel", "retry", "attempts", "events", "artifacts":
		if len(args) != 2 {
			return fmt.Errorf("job ID required")
		}
		path += "/" + args[1]
		if args[0] != "get" {
			path += "/" + args[0]
		}
		if args[0] == "cancel" || args[0] == "retry" {
			method = "POST"
		}
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
	req, e := http.NewRequest(method, strings.TrimRight(base, "/")+path, bytes.NewReader(body))
	if e != nil {
		return e
	}
	req.Header.Set("Content-Type", "application/json")
	rid := os.Getenv("RUNGRID_REQUEST_ID")
	if rid == "" {
		rid = scheduler.ID()
	}
	req.Header.Set("X-Request-ID", rid)
	client := &http.Client{Timeout: 30 * time.Second}
	res, e := client.Do(req)
	if e != nil {
		return fmt.Errorf("request %s: %w (reuse RUNGRID_REQUEST_ID for a safe retry)", rid, e)
	}
	defer res.Body.Close()
	_, e = io.Copy(os.Stdout, res.Body)
	if e != nil {
		return e
	}
	if res.StatusCode >= 400 {
		return fmt.Errorf("HTTP %d", res.StatusCode)
	}
	return nil
}
func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
