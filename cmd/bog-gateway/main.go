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

type syncFile struct{ *os.File }

func (f syncFile) Write(b []byte) (int, error) {
	n, e := f.File.Write(b)
	if e == nil {
		e = f.File.Sync()
	}
	return n, e
}
func main() {
	config := flag.String("config", "", "security configuration JSON")
	addr := flag.String("addr", "127.0.0.1:8443", "listen address")
	cert := flag.String("tls-cert", "", "TLS certificate")
	key := flag.String("tls-key", "", "TLS private key")
	flag.Parse()
	if *config == "" || *cert == "" || *key == "" {
		log.Fatal("config, tls-cert and tls-key are required")
	}
	b, e := os.ReadFile(*config)
	if e != nil {
		log.Fatal(e)
	}
	var c security.Config
	if e = json.Unmarshal(b, &c); e != nil {
		log.Fatal(e)
	}
	f, e := os.OpenFile(c.AuditPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if e != nil {
		log.Fatal(e)
	}
	defer f.Close()
	st, e := f.Stat()
	if e != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 {
		log.Fatal("audit file must be private and regular")
	}
	h, e := security.New(c, syncFile{f})
	if e != nil {
		log.Fatal(e)
	}
	s := &http.Server{Addr: *addr, Handler: h, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 60 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 32768}
	log.Fatal(s.ListenAndServeTLS(*cert, *key))
}
