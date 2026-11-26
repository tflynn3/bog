package main

import (
	"bog/internal/security"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	config := flag.String("config", "", "configuration JSON file")
	addr := flag.String("addr", "127.0.0.1:8443", "listen address")
	cert := flag.String("tls-cert", "", "TLS certificate")
	key := flag.String("tls-key", "", "TLS private key")
	flag.Parse()
	if *config == "" || *cert == "" || *key == "" {
		log.Fatal("config, tls-cert and tls-key are required")
	}
	b, err := os.ReadFile(*config)
	if err != nil {
		log.Fatal(err)
	}
	var c security.Config
	if err = json.Unmarshal(b, &c); err != nil {
		log.Fatal(err)
	}
	g, err := security.New(c)
	if err != nil {
		log.Fatal(err)
	}
	defer g.Close()
	s := &http.Server{Addr: *addr, Handler: g, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 32768}
	log.Fatal(s.ListenAndServeTLS(*cert, *key))
}
