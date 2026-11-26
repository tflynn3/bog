// bog-operator provisions a single, durable Bog member. Replicated operation is
// deliberately disabled until a fenced, durable replication protocol exists.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const sa = "/var/run/secrets/kubernetes.io/serviceaccount/"

type controller struct {
	client                                        *http.Client
	base, namespace, token, image, storage, class string
}

func newController() (*controller, error) {
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, fmt.Errorf("Kubernetes service address missing")
	}
	token, err := os.ReadFile(sa + "token")
	if err != nil {
		return nil, err
	}
	ns, err := os.ReadFile(sa + "namespace")
	if err != nil {
		return nil, err
	}
	ca, err := os.ReadFile(sa + "ca.crt")
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, fmt.Errorf("invalid cluster CA")
	}
	image := os.Getenv("BOG_IMAGE")
	if image == "" {
		return nil, fmt.Errorf("BOG_IMAGE required")
	}
	storage := os.Getenv("BOG_STORAGE")
	if storage == "" {
		storage = "10Gi"
	}
	replicas := os.Getenv("BOG_REPLICAS")
	if replicas != "" && replicas != "1" {
		return nil, fmt.Errorf("replication is unavailable; BOG_REPLICAS must be 1")
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	return &controller{&http.Client{Transport: tr, Timeout: 10 * time.Second}, "https://" + host + ":" + port, strings.TrimSpace(string(ns)), strings.TrimSpace(string(token)), image, storage, os.Getenv("BOG_STORAGE_CLASS")}, nil
}
func (c *controller) call(ctx context.Context, method, path string, value any) (int, []byte, error) {
	var body io.Reader
	if value != nil {
		b, err := json.Marshal(value)
		if err != nil {
			return 0, nil, err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, body)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	res, err := c.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer res.Body.Close()
	b, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	return res.StatusCode, b, err
}
func (c *controller) reconcile(ctx context.Context) error {
	// StatefulSet PVCs are retained by Kubernetes when the set is deleted.
	pvcSpec := map[string]any{"accessModes": []string{"ReadWriteOnce"}, "resources": map[string]any{"requests": map[string]string{"storage": c.storage}}}
	if c.class != "" {
		pvcSpec["storageClassName"] = c.class
	}
	labels := map[string]string{"app": "bog"}
	probe := map[string]any{"httpGet": map[string]any{"path": "/healthz", "port": 8080}, "periodSeconds": 10, "failureThreshold": 180}
	spec := map[string]any{"serviceName": "bog", "replicas": 1, "selector": map[string]any{"matchLabels": labels}, "template": map[string]any{"metadata": map[string]any{"labels": labels}, "spec": map[string]any{"nodeSelector": map[string]string{"kubernetes.io/os": "linux"}, "containers": []any{map[string]any{"name": "bog", "image": c.image, "args": []string{"-data-dir=/data", "-addr=0.0.0.0:8080"}, "ports": []any{map[string]any{"containerPort": 8080}}, "startupProbe": probe, "readinessProbe": map[string]any{"httpGet": map[string]any{"path": "/healthz", "port": 8080}}, "volumeMounts": []any{map[string]any{"name": "data", "mountPath": "/data"}}}}}}, "volumeClaimTemplates": []any{map[string]any{"metadata": map[string]string{"name": "data"}, "spec": pvcSpec}}}
	raw, _ := json.Marshal(spec)
	hash := sha256.Sum256(raw)
	digest := hex.EncodeToString(hash[:])
	meta := map[string]any{"name": "bog", "namespace": c.namespace, "annotations": map[string]string{"bog.io/config-hash": digest}}
	obj := map[string]any{"apiVersion": "apps/v1", "kind": "StatefulSet", "metadata": meta, "spec": spec}
	prefix := "/apis/apps/v1/namespaces/" + c.namespace + "/statefulsets"
	code, b, err := c.call(ctx, "GET", prefix+"/bog", nil)
	if err != nil {
		return err
	}
	if code == 404 {
		// A retained PVC may contain an existing member's durable state. Do not
		// silently attach it to a newly created workload with unknown history.
		pvcPath := "/api/v1/namespaces/" + c.namespace + "/persistentvolumeclaims/data-bog-0"
		pvcCode, _, pvcErr := c.call(ctx, "GET", pvcPath, nil)
		if pvcErr != nil {
			return pvcErr
		}
		if pvcCode != 404 {
			return fmt.Errorf("refusing to create StatefulSet: retained PVC check returned HTTP %d; inspect member state", pvcCode)
		}
		code, b, err = c.call(ctx, "POST", prefix, obj)
		if err != nil {
			return err
		}
		if code != 201 && code != 409 {
			return fmt.Errorf("create StatefulSet: HTTP %d: %s", code, b)
		}
		return nil
	}
	if code != 200 {
		return fmt.Errorf("get StatefulSet: HTTP %d: %s", code, b)
	}
	var current struct {
		Metadata struct {
			Annotations map[string]string `json:"annotations"`
		} `json:"metadata"`
		Spec struct {
			Replicas int `json:"replicas"`
		} `json:"spec"`
	}
	if err := json.Unmarshal(b, &current); err != nil {
		return err
	}
	if current.Spec.Replicas != 1 || current.Metadata.Annotations["bog.io/config-hash"] != digest {
		return fmt.Errorf("unsafe or incompatible StatefulSet change detected; manual intervention required")
	}
	return nil
}
func main() {
	c, err := newController()
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	interval := 15 * time.Second
	if s := os.Getenv("RECONCILE_SECONDS"); s != "" {
		n, e := strconv.Atoi(s)
		if e != nil || n < 5 {
			log.Fatal("RECONCILE_SECONDS must be at least 5")
		}
		interval = time.Duration(n) * time.Second
	}
	for {
		if err := c.reconcile(ctx); err != nil {
			log.Print(err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}
