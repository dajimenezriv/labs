package main

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
)

type Server struct {
	cfg     Config
	log     *slog.Logger
	queries *Queries
	issuer  *Issuer

	// Built once at construction rather than per request: it is derived
	// entirely from the signing key, so it cannot change while the process is
	// running. A rotation is a deploy, and a deploy rebuilds it.
	jwksDocument JWKS

	// Static keys, not a JWKS fetch: this service signed the tokens it is
	// checking, so the key is already in hand. See requireAuth.
	verifier *Verifier
}

func NewServer(cfg Config, log *slog.Logger, queries *Queries, issuer *Issuer) *Server {
	return &Server{
		cfg:          cfg,
		log:          log,
		queries:      queries,
		issuer:       issuer,
		jwksDocument: NewJWKS(issuer.PublicKey()),
		verifier:     NewVerifier(NewStaticKeys(issuer.PublicKey()))}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// Publishes the public half of the signing key.
	mux.HandleFunc("GET /.well-known/jwks.json", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=3600")
		s.writeJSON(w, r, http.StatusOK, s.jwksDocument)
	})

	mux.HandleFunc("POST /login", s.login)
	mux.HandleFunc("POST /logout", s.logout)
	mux.HandleFunc("POST "+refreshCookiePath, s.refresh)

	return mux
}

// maxBodyBytes caps a request body. No endpoint here accepts more than a few
// hundred bytes of JSON, and without a cap a client can make the server read
// for as long as it cares to send.
const maxBodyBytes = 1 << 20

// FieldError says which part of the request was rejected and why.
type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

// ErrorResponse is the body every failure returns, so a client has one shape to
// parse whatever went wrong.
type ErrorResponse struct {
	Error  string       `json:"error"`
	Fields []FieldError `json:"fields,omitempty"`
}

// apiError is a failure that carries the status to report it as. Handlers and
// the helpers they call return it as an ordinary error, and fail turns it into
// a response at the one point that writes one.
type apiError struct {
	status  int
	message string
	fields  []FieldError
}

func (e apiError) Error() string { return e.message }

var (
	errUnauthorized = apiError{status: http.StatusUnauthorized}
	errInternal     = apiError{status: http.StatusInternalServerError}
)

// invalidRequest reports failed validation. The field list is safe to return:
// it describes what the caller sent, not anything they did not already know.
func invalidRequest(fields []FieldError) error {
	return apiError{status: http.StatusBadRequest, message: "invalid request", fields: fields}
}

// fail writes err as a response. Anything that is not an apiError is a bug
// rather than a rejection, so it becomes a 500 with nothing in the body: the
// caller cannot act on it and it may describe our internals.
func (s *Server) fail(w http.ResponseWriter, r *http.Request, err error) {
	var e apiError
	if !errors.As(err, &e) {
		s.log.ErrorContext(r.Context(), "unclassified handler error", "err", err)
		e = errInternal
	}

	message := e.message
	if message == "" {
		message = http.StatusText(e.status)
	}

	s.writeJSON(w, r, e.status, ErrorResponse{Error: message, Fields: e.fields})
}

func (s *Server) writeJSON(w http.ResponseWriter, r *http.Request, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	// Nothing can be done about a failure here — the status is already on the
	// wire — but it is worth knowing the client did not get what we sent.
	if err := json.NewEncoder(w).Encode(body); err != nil {
		s.log.ErrorContext(r.Context(), "write response", "err", err, "status", status)
	}
}

func writeError(w http.ResponseWriter, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusInternalServerError)
	json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}
