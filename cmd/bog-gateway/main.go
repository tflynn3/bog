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
	config := flag.String("config", "", "path to private gateway JSON configuration")
	addr := flag.String("addr", "127.0.0.1:8443", "HTTPS listen address")
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
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 {
		log.Fatal("configuration must be a private regular file")
	}
	var c security.Config
	d := json.NewDecoder(f)
	d.DisallowUnknownFields()
	if err = d.Decode(&c); err != nil {
		log.Fatal(err)
	}
	f.Close()
	gateway, err := security.New(c)
	if err != nil {
		log.Fatal(err)
	}
	defer gateway.Close()
	server := &http.Server{Addr: *addr, Handler: gateway, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 60 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 32768}
	log.Fatal(server.ListenAndServeTLS(*cert, *key))
}
