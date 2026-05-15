package main

import (
    "encoding/base64"
    "fmt"
    "io"
    "log"
    "net"
    "net/http"
    "os"

    "lukechampine.com/blake3"
)

func main() {
    path := "/run/sht/sht.sock"

    os.Remove(path)

    ln, err := net.Listen("unix", path)
    if err != nil {
        log.Fatal(err)
    }
    defer ln.Close()

    if err := os.Chmod(path, 0666); err != nil {
        log.Fatal(err)
    }

    mux := http.NewServeMux()
    mux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
        fmt.Fprintln(w, "pong")
    })

    mux.HandleFunc("/blobs", func(w http.ResponseWriter, r *http.Request) {
        if r.Method != http.MethodPost {
            http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
            return
        }

        defer r.Body.Close()

        h := blake3.New(32, nil)
        _, err := io.Copy(h, r.Body)
        if err != nil {
            http.Error(w, "hash failed", http.StatusInternalServerError)
            return
        }

        sum := h.Sum(nil)
        dgst := base64.RawURLEncoding.EncodeToString(sum)
        fmt.Fprintln(w, dgst)
    })

    log.Println("Listening on ", path)
    log.Fatal(http.Serve(ln, mux))
}
