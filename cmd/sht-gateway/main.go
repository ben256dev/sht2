package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"

	"sht/internal/admin"
)

type config struct {
	addr               string
	dbPath             string
	authorizedKeysPath string
	sockDir            string
	oidcIssuer         string
	oidcClientID       string
	usernameClaim      string
	devAuth            bool
}

type server struct {
	cfg      config
	db       *sql.DB
	client   *http.Client
	verifier *oidc.IDTokenVerifier
}

type identity struct {
	Subject  string
	Username string
}

type claims struct {
	Subject string `json:"sub"`
}

type keyCreateRequest struct {
	Name      string `json:"name"`
	PublicKey string `json:"public_key"`
}

func getenv(key, fallback string) string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	return v
}

func loadConfig() config {
	return config{
		addr:               getenv("SHT_GATEWAY_ADDR", ":8080"),
		dbPath:             getenv("SHT_DB_PATH", admin.DefaultDBPath),
		authorizedKeysPath: os.Getenv("SHT_AUTHORIZED_KEYS_PATH"),
		sockDir:            getenv("SHT_SOCK_DIR", "/run/sht/sht.sock"),
		oidcIssuer:         os.Getenv("SHT_OIDC_ISSUER"),
		oidcClientID:       os.Getenv("SHT_OIDC_CLIENT_ID"),
		usernameClaim:      getenv("SHT_OIDC_USERNAME_CLAIM", "preferred_username"),
		devAuth:            os.Getenv("SHT_GATEWAY_DEV_AUTH") == "1",
	}
}

func newServer(ctx context.Context, cfg config) (*server, error) {
	db, err := admin.OpenDB(cfg.dbPath)
	if err != nil {
		return nil, err
	}
	s := &server{cfg: cfg, db: db}
	s.client = &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", cfg.sockDir)
		},
	}}
	if !cfg.devAuth {
		if cfg.oidcIssuer == "" || cfg.oidcClientID == "" {
			db.Close()
			return nil, fmt.Errorf("SHT_OIDC_ISSUER and SHT_OIDC_CLIENT_ID are required unless SHT_GATEWAY_DEV_AUTH=1")
		}
		provider, err := oidc.NewProvider(ctx, cfg.oidcIssuer)
		if err != nil {
			db.Close()
			return nil, err
		}
		s.verifier = provider.Verifier(&oidc.Config{ClientID: cfg.oidcClientID})
	}
	return s, nil
}

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /me", s.withAuth(s.me))
	mux.HandleFunc("GET /keys", s.withAuth(s.keys))
	mux.HandleFunc("POST /keys", s.withAuth(s.createKey))
	mux.HandleFunc("GET /blob/{digest}", s.publicBlob)
	mux.HandleFunc("HEAD /blob/{digest}", s.publicBlob)
	mux.HandleFunc("POST /blob", s.withAuth(s.uploadBlob))
	mux.HandleFunc("POST /uploads", s.withAuth(s.uploads))
	mux.HandleFunc("GET /uploads/{digest}", s.withAuth(s.uploads))
	mux.HandleFunc("POST /uploads/{digest}/finalize", s.withAuth(s.uploads))
	mux.HandleFunc("PUT /uploads/{digest}/chunks/{index}", s.withAuth(s.uploads))
	return mux
}

func (s *server) close() {
	_ = s.db.Close()
}

func (s *server) healthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *server) me(w http.ResponseWriter, r *http.Request, ident identity) {
	u, err := admin.CreateMappedUser(s.db, ident.Subject, ident.Username)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"user": u})
}

func (s *server) keys(w http.ResponseWriter, r *http.Request, ident identity) {
	u, err := admin.CreateMappedUser(s.db, ident.Subject, ident.Username)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	keys, err := admin.ListKeysForUserID(s.db, u.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if keys == nil {
		keys = []admin.Key{}
	}
	publicKeys := keys[:0]
	for _, key := range keys {
		if key.PublicKey != "" {
			publicKeys = append(publicKeys, key)
		}
	}
	keys = publicKeys
	writeJSON(w, http.StatusOK, map[string]any{"keys": keys})
}

func (s *server) createKey(w http.ResponseWriter, r *http.Request, ident identity) {
	var req keyCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	u, err := admin.CreateMappedUser(s.db, ident.Subject, ident.Username)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	key, err := admin.AddKeyForUserID(s.db, u.ID, u.Name, req.Name, req.PublicKey)
	if err != nil {
		status := http.StatusBadRequest
		if strings.Contains(err.Error(), "already exists") {
			status = http.StatusConflict
		}
		writeError(w, status, err.Error())
		return
	}
	if s.cfg.authorizedKeysPath != "" {
		if err := admin.SyncAuthorizedKeys(s.db, s.cfg.authorizedKeysPath); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
	}
	writeJSON(w, http.StatusCreated, map[string]any{"key": key})
}

func (s *server) publicBlob(w http.ResponseWriter, r *http.Request) {
	digest := r.PathValue("digest")
	s.proxySHTD(w, r, "/public/blob/"+digest, 0, "")
}

func (s *server) uploadBlob(w http.ResponseWriter, r *http.Request, ident identity) {
	keyID, err := s.gatewayKeyID(ident)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	s.proxySHTD(w, r, "/blob", keyID, requestShelf(r))
}

func (s *server) uploads(w http.ResponseWriter, r *http.Request, ident identity) {
	keyID, err := s.gatewayKeyID(ident)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	s.proxySHTD(w, r, r.URL.Path, keyID, requestShelf(r))
}

func (s *server) gatewayKeyID(ident identity) (int64, error) {
	u, err := admin.CreateMappedUser(s.db, ident.Subject, ident.Username)
	if err != nil {
		return 0, err
	}
	return admin.EnsureGatewayKeyForUser(s.db, u.ID)
}

func requestShelf(r *http.Request) string {
	if shelf := strings.TrimSpace(r.Header.Get("X-SHT-Shelf")); shelf != "" {
		return shelf
	}
	return strings.TrimSpace(r.URL.Query().Get("shelf"))
}

func (s *server) proxySHTD(w http.ResponseWriter, r *http.Request, path string, keyID int64, shelf string) {
	req, err := http.NewRequestWithContext(r.Context(), r.Method, "http://sht"+path, r.Body)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if contentType := r.Header.Get("Content-Type"); contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if r.ContentLength >= 0 {
		req.ContentLength = r.ContentLength
	}
	if keyID > 0 {
		req.Header.Set("X-SHT-Key-ID", fmt.Sprintf("%d", keyID))
	}
	if shelf != "" {
		req.Header.Set("X-SHT-Shelf", shelf)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	defer resp.Body.Close()

	for key, values := range resp.Header {
		if strings.EqualFold(key, "Connection") {
			continue
		}
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(resp.StatusCode)
	if r.Method != http.MethodHead {
		_, _ = io.Copy(w, resp.Body)
	}
}

func (s *server) withAuth(next func(http.ResponseWriter, *http.Request, identity)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ident, err := s.authenticate(r)
		if err != nil {
			writeError(w, http.StatusUnauthorized, err.Error())
			return
		}
		next(w, r, ident)
	}
}

func (s *server) authenticate(r *http.Request) (identity, error) {
	if s.cfg.devAuth {
		subject := strings.TrimSpace(r.Header.Get("X-SHT-Dev-Subject"))
		if subject == "" {
			return identity{}, fmt.Errorf("missing dev subject")
		}
		return identity{Subject: subject, Username: strings.TrimSpace(r.Header.Get("X-SHT-Dev-Username"))}, nil
	}
	auth := r.Header.Get("Authorization")
	token := strings.TrimPrefix(auth, "Bearer ")
	if token == auth || token == "" {
		return identity{}, fmt.Errorf("missing bearer token")
	}
	idToken, err := s.verifier.Verify(r.Context(), token)
	if err != nil {
		return identity{}, fmt.Errorf("invalid bearer token")
	}
	var base claims
	if err := idToken.Claims(&base); err != nil {
		return identity{}, fmt.Errorf("invalid claims")
	}
	var raw map[string]any
	if err := idToken.Claims(&raw); err != nil {
		return identity{}, fmt.Errorf("invalid claims")
	}
	username, _ := raw[s.cfg.usernameClaim].(string)
	return identity{Subject: base.Subject, Username: username}, nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{"error": map[string]any{"status": status, "message": message}})
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s, err := newServer(ctx, loadConfig())
	if err != nil {
		log.Fatal(err)
	}
	defer s.close()

	log.Printf("Listening on %s", s.cfg.addr)
	if err := http.ListenAndServe(s.cfg.addr, s.routes()); err != nil {
		log.Fatal(err)
	}
}
