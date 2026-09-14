package api

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"

	"github.com/ZubairQazi/rungrid/internal/scheduler"
	"github.com/ZubairQazi/rungrid/internal/store"
)

var uuid = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func decode(w http.ResponseWriter, r *http.Request, v any) error {
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20))
	d.DisallowUnknownFields()
	if e := d.Decode(v); e != nil {
		return e
	}
	var extra any
	if e := d.Decode(&extra); e != io.EOF {
		return store.ErrInvalid
	}
	return nil
}
func reply(w http.ResponseWriter, v any, e error) {
	w.Header().Set("Content-Type", "application/json")
	if e != nil {
		code := 500
		message := "internal error"
		switch {
		case errors.Is(e, store.ErrInvalid):
			code = 400
			message = e.Error()
		case errors.Is(e, store.ErrNotFound):
			code = 404
			message = e.Error()
		case errors.Is(e, store.ErrConflict):
			code = 409
			message = e.Error()
		default:
			slog.Error("request failed", "error", e)
		}
		w.WriteHeader(code)
		json.NewEncoder(w).Encode(map[string]string{"error": message})
		return
	}
	json.NewEncoder(w).Encode(v)
}
func page(r *http.Request) (int, int, error) {
	limit := 100
	offset := 0
	var e error
	if v := r.URL.Query().Get("limit"); v != "" {
		limit, e = strconv.Atoi(v)
		if e != nil {
			return 0, 0, store.ErrInvalid
		}
	}
	if v := r.URL.Query().Get("offset"); v != "" {
		offset, e = strconv.Atoi(v)
		if e != nil {
			return 0, 0, store.ErrInvalid
		}
	}
	if limit < 1 || limit > 1000 || offset < 0 {
		return 0, 0, store.ErrInvalid
	}
	return limit, offset, nil
}
func Handler(s *store.Store, metrics http.Handler) http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { reply(w, map[string]string{"status": "ok"}, nil) })
	m.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if e := s.Pool.Ping(r.Context()); e != nil {
			http.Error(w, "database unavailable", 503)
			return
		}
		reply(w, map[string]string{"status": "ready"}, nil)
	})
	if metrics != nil {
		m.Handle("GET /metrics", metrics)
	}
	m.HandleFunc("POST /v1/jobs", func(w http.ResponseWriter, r *http.Request) {
		var j scheduler.Job
		if e := decode(w, r, &j); e != nil {
			reply(w, nil, store.ErrInvalid)
			return
		}
		rows, e := s.Submit(r.Context(), []scheduler.Job{j})
		if e != nil {
			reply(w, nil, e)
			return
		}
		reply(w, rows[0], nil)
	})
	m.HandleFunc("POST /v1/jobs/batch", func(w http.ResponseWriter, r *http.Request) {
		var jobs []scheduler.Job
		if e := decode(w, r, &jobs); e != nil {
			reply(w, nil, store.ErrInvalid)
			return
		}
		v, e := s.Submit(r.Context(), jobs)
		reply(w, v, e)
	})
	m.HandleFunc("GET /v1/jobs", func(w http.ResponseWriter, r *http.Request) {
		l, o, e := page(r)
		if e != nil {
			reply(w, nil, e)
			return
		}
		v, e := s.List(r.Context(), l, o)
		reply(w, v, e)
	})
	m.HandleFunc("GET /v1/jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
		if !uuid.MatchString(r.PathValue("id")) {
			reply(w, nil, store.ErrInvalid)
			return
		}
		v, e := s.Get(r.Context(), r.PathValue("id"))
		reply(w, v, e)
	})
	for _, kind := range []string{"attempts", "events", "artifacts"} {
		m.HandleFunc("GET /v1/jobs/{id}/"+kind, func(w http.ResponseWriter, r *http.Request) {
			id := r.PathValue("id")
			if !uuid.MatchString(id) {
				reply(w, nil, store.ErrInvalid)
				return
			}
			l, o, e := page(r)
			if e != nil {
				reply(w, nil, e)
				return
			}
			if _, e = s.Get(r.Context(), id); e != nil {
				reply(w, nil, e)
				return
			}
			v, e := s.ReadRows(r.Context(), kind, id, l, o)
			reply(w, v, e)
		})
	}
	for _, action := range []string{"cancel", "retry"} {
		m.HandleFunc("POST /v1/jobs/{id}/"+action, func(w http.ResponseWriter, r *http.Request) {
			id := r.PathValue("id")
			if !uuid.MatchString(id) {
				reply(w, nil, store.ErrInvalid)
				return
			}
			v, e := s.Action(r.Context(), id, action, r.Header.Get("X-Request-ID"))
			reply(w, v, e)
		})
	}
	m.HandleFunc("GET /v1/workers", func(w http.ResponseWriter, r *http.Request) { v, e := s.Workers(r.Context()); reply(w, v, e) })
	return m
}
