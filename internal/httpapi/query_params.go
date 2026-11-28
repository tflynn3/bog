package httpapi

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"

	"bog/internal/store"
)

type queryParams struct {
	filter PageFilter
	limit  int
	page   bool
	cursor string
}

var decimal = regexp.MustCompile(`^[0-9]+$`)

func parseQuery(r *http.Request, tokenSize int) (queryParams, error) {
	var params queryParams
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return params, errors.New("invalid query")
	}
	known := 0
	for _, field := range [...]string{"start", "end", "contains", "limit", "page", "cursor"} {
		if values, present := q[field]; present {
			known++
			if len(values) != 1 {
				return params, fmt.Errorf("invalid %s: must appear only once", field)
			}
		}
	}
	if known != len(q) {
		return params, errors.New("invalid parameter")
	}
	if !q.Has("start") {
		return params, errors.New("missing start")
	}
	start, err := store.ParseTime(q.Get("start"))
	if err != nil {
		return params, errors.New("invalid start: expected an RFC3339 timestamp")
	}
	if !q.Has("end") {
		return params, errors.New("missing end")
	}
	end, err := store.ParseTime(q.Get("end"))
	if err != nil {
		return params, errors.New("invalid end: expected an RFC3339 timestamp")
	}
	if end.Before(start) {
		return params, errors.New("invalid range: end precedes start")
	}
	limit := 500
	if q.Has("limit") {
		value := q.Get("limit")
		if !decimal.MatchString(value) {
			return params, errors.New("invalid limit: expected decimal digits")
		}
		limit, err = strconv.Atoi(value)
		if err != nil || limit < 1 || limit > maxQueryLimit {
			return params, fmt.Errorf("invalid limit: must be between 1 and %d", maxQueryLimit)
		}
	}
	page, continuation := q.Has("page"), q.Has("cursor")
	cursor := q.Get("cursor")
	if (page && (q.Get("page") != "1" || continuation)) || (continuation && cursor == "") {
		return params, errors.New("invalid pagination")
	}
	// Reserve room for the cursor and the continuation's headers within the
	// serving listener's configured limit.
	headerLimit := http.DefaultMaxHeaderBytes
	if server, ok := r.Context().Value(http.ServerContextKey).(*http.Server); ok && server.MaxHeaderBytes > 0 {
		headerLimit = server.MaxHeaderBytes
	}
	if page && len(r.RequestURI)+tokenSize+4096 > headerLimit {
		return params, errors.New("query too large for continuation")
	}
	return queryParams{PageFilter{start, end, q.Get("contains")}, limit, page, cursor}, nil
}
