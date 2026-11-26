package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	"bog/internal/security"
)

func main() {
	config := flag.String("config", "", "JSON configuration file")
	addr := flag.String("addr", "127.0.0.1:8443", "listen address")
	cert := flag.String("tls-cert", "", "TLS certificate")
	key := flag.String("tls-key", "", "TLS private key")
	flag.Parse()
	if *config == "" || *cert == "" || *key == "" {
		log.Fatal("config, tls-cert and tls-key are required")
	}
	f, err := os.Open(*config)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	var c security.Config
	d := json.NewDecoder(f)
	d.DisallowUnknownFields()
	if err = d.Decode(&c); err != nil {
		log.Fatal(err)
	}
	g, err := security.New(c)
	if err != nil {
		log.Fatal(err)
	}
	defer g.Close()
	srv := &http.Server{Addr: *addr, Handler: g, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 32 << 10}
	log.Fatal(srv.ListenAndServeTLS(*cert, *key))
}
