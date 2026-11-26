// The proxy deliberately rejects writes until Bog exposes a fenced replicated log.
package main

import (
	"flag"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"
)

func main() {
	listen := flag.String("listen", ":8080", "proxy listen address")
	backend := flag.String("backend", "http://127.0.0.1:8081", "local Bog endpoint")
	flag.Parse()
	u, err := url.Parse(*backend)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		log.Fatal("invalid backend URL")
	}
	proxy := httputil.NewSingleHostReverseProxy(u)
	mux := http.NewServeMux()
	mux.HandleFunc("/ingest", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "replicated writes unavailable: no consensus protocol", http.StatusServiceUnavailable)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "replication unavailable", http.StatusServiceUnavailable)
	})
	mux.Handle("/query", proxy)
	mux.Handle("/healthz", proxy)
	server := &http.Server{Addr: *listen, Handler: mux, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	log.Fatal(server.ListenAndServe())
}
