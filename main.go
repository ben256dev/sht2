package main

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/joho/godotenv"
	"github.com/zeebo/blake3"
)

var (
	root = getenv("BLOB_PATH", "/home/sht2")
	idRe = regexp.MustCompile(`^[A-Za-z0-9_-]{43}$`)

	cfgPath = ".sht2"

	quotaMu sync.Mutex
	limits  quotaConfig
)

type quotaConfig struct {
	MaxStorageGB   float64
	MaxUploadGB    float64
	MaxStorageByte int64
	MaxUploadByte  int64
}

const gib = int64(1024 * 1024 * 1024)

func getenv(k, d string) string {
	v := os.Getenv(k)
	if v == "" {
		return d
	}
	return v
}

func objPath(id string) string {
	return filepath.Join(root, id[:2], id[2:4], id)
}

func defaultQuotaConfig() quotaConfig {
	return quotaConfig{
		MaxStorageGB:   20,
		MaxUploadGB:    2,
		MaxStorageByte: 20 * gib,
		MaxUploadByte:  2 * gib,
	}
}

func writeDefaultConfig(path string, cfg quotaConfig) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = fmt.Fprintf(f,
		"# sht2 storage configuration (values are in GiB)\nMAX_STORAGE_GB=%.0f\nMAX_UPLOAD_GB=%.0f\n",
		cfg.MaxStorageGB, cfg.MaxUploadGB,
	)
	return err
}

func loadQuotaConfig() (quotaConfig, error) {
	cfg := defaultQuotaConfig()
	path := filepath.Join(root, cfgPath)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := writeDefaultConfig(path, cfg); err != nil {
			return cfg, err
		}
		return cfg, nil
	}

	f, err := os.Open(path)
	if err != nil {
		return cfg, err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		gb, err := strconv.ParseFloat(val, 64)
		if err != nil || gb <= 0 {
			return cfg, fmt.Errorf("invalid %s in %s", key, path)
		}
		switch key {
		case "MAX_STORAGE_GB":
			cfg.MaxStorageGB = gb
			cfg.MaxStorageByte = int64(gb * float64(gib))
		case "MAX_UPLOAD_GB":
			cfg.MaxUploadGB = gb
			cfg.MaxUploadByte = int64(gb * float64(gib))
		}
	}
	if err := sc.Err(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func diskUsageBytes(exclude string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if path == exclude || filepath.Base(path) == cfgPath {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	return total, err
}

func upload(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, limits.MaxUploadByte)

	tmp, err := os.CreateTemp(root, "up-*")
	if err != nil {
		http.Error(w, "tmp", 500)
		return
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	src := io.Reader(r.Body)
	contentType := r.Header.Get("Content-Type")
	if contentType != "" {
		mediaType, _, err := mime.ParseMediaType(contentType)
		if err != nil {
			http.Error(w, "invalid content-type", http.StatusBadRequest)
			return
		}
		if mediaType == "multipart/form-data" {
			mr, err := r.MultipartReader()
			if err != nil {
				http.Error(w, "invalid multipart body", http.StatusBadRequest)
				return
			}

			var filePart io.ReadCloser
			for {
				part, err := mr.NextPart()
				if err == io.EOF {
					break
				}
				if err != nil {
					http.Error(w, "invalid multipart body", http.StatusBadRequest)
					return
				}

				if part.FileName() != "" || part.FormName() == "file" {
					filePart = part
					break
				}
				_ = part.Close()
			}

			if filePart == nil {
				http.Error(w, "multipart missing file part", http.StatusBadRequest)
				return
			}
			defer filePart.Close()
			src = filePart
		}
	}

	h := blake3.New()
	n, err := io.Copy(io.MultiWriter(tmp, h), src)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			http.Error(w, "upload exceeds MAX_UPLOAD_GB", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "copy", 400)
		return
	}
	if err := tmp.Close(); err != nil {
		http.Error(w, "close", 500)
		return
	}

	id := base64.RawURLEncoding.EncodeToString(h.Sum(nil))
	final := objPath(id)

	if err := os.MkdirAll(filepath.Dir(final), 0755); err != nil {
		http.Error(w, "mkdir", 500)
		return
	}

	quotaMu.Lock()
	defer quotaMu.Unlock()

	if _, err := os.Stat(final); err == nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"id": id, "size": n, "deduped": true})
		return
	}

	used, err := diskUsageBytes(tmpName)
	if err != nil {
		http.Error(w, "usage", 500)
		return
	}
	if used+n > limits.MaxStorageByte {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInsufficientStorage)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error":             "root storage limit exceeded",
			"used_bytes":        used,
			"max_storage_bytes": limits.MaxStorageByte,
		})
		return
	}

	if err := os.Rename(tmpName, final); err != nil {
		http.Error(w, "rename", 500)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"id": id, "size": n, "deduped": false})
}

func serveByID(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/")
	if id == "" || strings.Contains(id, "/") || !idRe.MatchString(id) {
		http.NotFound(w, r)
		return
	}
	p := objPath(id)

	f, err := os.Open(p)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		http.Error(w, "stat", 500)
		return
	}

	w.Header().Set("ETag", `"`+id+`"`)
	http.ServeContent(w, r, id, st.ModTime(), f)
}

func setCORS(w http.ResponseWriter, r *http.Request) {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return
	}

	// For dev/open use:
	// w.Header().Set("Access-Control-Allow-Origin", "*")

	// Safer: reflect exact origin (works with credentials too)
	w.Header().Set("Vary", "Origin, Access-Control-Request-Headers, Access-Control-Request-Method")
	w.Header().Set("Access-Control-Allow-Origin", origin)
	w.Header().Set("Access-Control-Allow-Methods", "POST, PUT, GET, HEAD, OPTIONS")
	reqHeaders := r.Header.Get("Access-Control-Request-Headers")
	if reqHeaders != "" {
		w.Header().Set("Access-Control-Allow-Headers", reqHeaders)
	} else {
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With, Cache-Control, Pragma")
	}
	// Uncomment only if needed:
	// w.Header().Set("Access-Control-Allow-Credentials", "true")
	w.Header().Set("Access-Control-Max-Age", "86400")
}

func main() {
	_ = godotenv.Load()

	root = getenv("BLOB_PATH", "/home/sht2")

	port := getenv("PORT", "8080")

	_ = os.MkdirAll(root, 0755)
	cfg, err := loadQuotaConfig()
	if err != nil {
		log.Fatal(err)
	}
	limits = cfg

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		setCORS(w, r)

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		if r.URL.Path == "/" {
			if r.Method == "POST" || r.Method == "PUT" {
				upload(w, r)
				return
			}
			w.WriteHeader(405)
			return
		}

		if r.Method == "GET" || r.Method == "HEAD" {
			serveByID(w, r)
			return
		}

		w.WriteHeader(405)
	})

	fmt.Printf("listening on :%s (max_storage=%.2fGiB max_upload=%.2fGiB)\n", port, limits.MaxStorageGB, limits.MaxUploadGB)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal(err)
	}
}
