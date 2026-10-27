// Package api serves the HTTP ingest and debug endpoints.
package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"regexp"

	"github.com/7vignesh/mailq/internal/clock"
	"github.com/7vignesh/mailq/internal/metrics"
	"github.com/7vignesh/mailq/internal/store"
)

const maxBodyBytes = 1 << 20 // 1 MiB request limit, including the message body

type Server struct {
	store   *store.Store
	clock   clock.Clock
	log     *slog.Logger
	metrics *metrics.API
}

func New(st *store.Store, clk clock.Clock, log *slog.Logger, m *metrics.API) *Server {
	return &Server{store: st, clock: clk, log: log, metrics: m}
}

func (s *Server) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /messages", s.postMessage)
	mux.HandleFunc("GET /messages/{id}", s.getMessage)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	return mux
}

type postRequest struct {
	From           string `json:"from"`
	To             string `json:"to"`
	Subject        string `json:"subject"`
	Body           string `json:"body"`
	IdempotencyKey string `json:"idempotency_key"`
}

// postMessage returns 202 for a new message, 200 with the original message
// for a repeated idempotency key, and 409 if the key was reused with a
// different payload.
func (s *Server) postMessage(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req postRequest
	if err := dec.Decode(&req); err != nil {
		s.metrics.Ingest.WithLabelValues("invalid").Inc()
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	nm, err := validate(req)
	if err != nil {
		s.metrics.Ingest.WithLabelValues("invalid").Inc()
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}

	m, created, err := s.store.Enqueue(r.Context(), nm, s.clock.Now())
	switch {
	case errors.Is(err, store.ErrIdempotencyConflict):
		s.metrics.Ingest.WithLabelValues("conflict").Inc()
		writeJSON(w, http.StatusConflict, map[string]any{
			"error": "idempotency_key was already used with a different payload", "message": summary(m)})
		return
	case err != nil:
		s.metrics.Ingest.WithLabelValues("error").Inc()
		s.log.Error("enqueue failed", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusAccepted
		s.metrics.Ingest.WithLabelValues("created").Inc()
		s.log.Info("message accepted", "message_id", m.ID, "domain", m.Domain)
	} else {
		s.metrics.Ingest.WithLabelValues("duplicate").Inc()
	}
	writeJSON(w, status, summary(m))
}

var uuidRE = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// getMessage returns the message and every attempt, for debugging a
// delivery by domain, MX host and reply code.
func (s *Server) getMessage(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !uuidRE.MatchString(id) {
		writeError(w, http.StatusNotFound, "message not found")
		return
	}
	m, attempts, err := s.store.Get(r.Context(), id)
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "message not found")
		return
	case err != nil:
		s.log.Error("get failed", "message_id", id, "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if attempts == nil {
		attempts = []store.Attempt{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"message": m, "attempts": attempts})
}

func summary(m store.Message) map[string]any {
	return map[string]any{"id": m.ID, "idempotency_key": m.IdempotencyKey, "state": m.State,
		"domain": m.Domain, "created_at": m.CreatedAt}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
