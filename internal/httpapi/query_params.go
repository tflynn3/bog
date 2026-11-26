package httpapi

import (
	"errors"
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

func parseQuery(r *http.Request) (queryParams, error) {
	var params queryParams
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return params, errors.New("invalid query")
	}
	for k, values := range q {
		switch k {
		case "start", "end", "contains", "limit", "page", "cursor":
		default:
			return params, errors.New("invalid parameter")
		}
		if len(values) != 1 {
			return params, errors.New("invalid parameter")
		}
	}
	start, err := store.ParseTime(q.Get("start"))
	if err != nil {
		return params, errors.New("invalid start")
	}
	end, err := store.ParseTime(q.Get("end"))
	if err != nil {
		return params, errors.New("invalid end")
	}
	if end.Before(start) {
		return params, errors.New("invalid range: end precedes start")
	}
	limit := 500
	if q.Has("limit") {
		value := q.Get("limit")
		limit, err = strconv.Atoi(value)
		if err != nil || !decimal.MatchString(value) || limit < 1 || limit > 1000 {
			return params, errors.New("invalid limit")
		}
	}
	page, continuation := q.Has("page"), q.Has("cursor")
	cursor := q.Get("cursor")
	if (page && (q.Get("page") != "1" || continuation)) || (continuation && cursor == "") {
		return params, errors.New("invalid pagination")
	}
	// Reserve room for the largest cursor and ordinary headers within the
	// server's 1 MiB header limit, using the original request target.
	if page && len(r.RequestURI)+maxCursorTokenBytes+4096 > 1<<20 {
		return params, errors.New("query too large for continuation")
	}
	return queryParams{PageFilter{start, end, q.Get("contains")}, limit, page, cursor}, nil
}
