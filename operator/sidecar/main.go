// The sidecar is a single-upstream HTTP proxy. It does not replicate writes.
package main

import (
	"bytes"
	"flag"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"
)

const maxBatch = 4 << 20

func main() {
	listen := flag.String("listen", ":8081", "listen address")
	upstream := flag.String("upstream", "http://127.0.0.1:8080", "single Bog process")
	flag.Parse()
	u, err := url.Parse(*upstream)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		log.Fatal("invalid upstream")
	}
	proxy := httputil.NewSingleHostReverseProxy(u)
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("upstream: %v", err)
		http.Error(w, "upstream outcome unknown", http.StatusBadGateway)
	}
	slots := make(chan struct{}, 16)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ingest" && r.Method == http.MethodPost {
			select {
			case slots <- struct{}{}:
				defer func() { <-slots }()
			default:
				http.Error(w, "busy", http.StatusServiceUnavailable)
				return
			}
			if r.ContentLength > maxBatch {
				http.Error(w, "batch too large", http.StatusRequestEntityTooLarge)
				return
			}
			b, err := io.ReadAll(io.LimitReader(r.Body, maxBatch+1))
			r.Body.Close()
			if len(b) > maxBatch {
				http.Error(w, "batch too large", http.StatusRequestEntityTooLarge)
				return
			}
			if err != nil || (r.ContentLength >= 0 && int64(len(b)) != r.ContentLength) {
				http.Error(w, "incomplete body", http.StatusBadRequest)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(b))
			r.ContentLength = int64(len(b))
		}
		proxy.ServeHTTP(w, r)
	})
	s := &http.Server{Addr: *listen, Handler: handler, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 1 << 20}
	log.Fatal(s.ListenAndServe())
}
