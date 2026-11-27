package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"time"
)

func ExampleBoundedWalk() {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body string
		switch params := r.URL.Query(); {
		case params.Get("page") == "1":
			w.Header().Set(nextCursorHeader, fixtureCursor)
			body = contractRow("first")
		case params.Get("cursor") == fixtureCursor:
			body = contractRow("last")
		default:
			http.Error(w, "invalid cursor", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		if _, err := io.WriteString(w, body); err != nil {
			panic(err)
		}
	}))
	defer server.Close()

	c, err := New(server.URL, nil)
	if err != nil {
		panic(err)
	}
	defer c.CloseIdleConnections()

	// Start one deadline for the walk, including page reads and retry waits.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	query := Query{
		Start: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		End:   time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC),
		Limit: 1,
	}
	walk, err := c.NewBoundedWalkWithContext(ctx, query, 10)
	if err != nil {
		panic(err)
	}

	retry := PageRetry{MaxRetries: 2, Delay: 100 * time.Millisecond}
	for {
		page, err := walk.NextPageWithRetry(retry)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			panic(err)
		}
		for _, row := range page.Rows {
			fmt.Println(row.Line)
		}
	}

	// Output:
	// first
	// last
}
