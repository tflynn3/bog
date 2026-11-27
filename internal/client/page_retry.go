package client

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrPageRetryExhausted means all configured attempts to fetch a retryable page
// failed. The returned error also wraps the last fetch error.
var ErrPageRetryExhausted = errors.New("query page retries exhausted")

// PageRetry bounds retries separately for each page. MaxRetries counts attempts
// after the initial request. Delay is the fixed wait between attempts.
// Both values must be nonnegative. The zero value makes a single attempt.
type PageRetry struct {
	MaxRetries int
	Delay      time.Duration
}

// NextPageWithRetry fetches a page using NextPage. It retries query HTTP 503 and
// interrupted delivery of otherwise valid HTTP 200 pages. Each retry uses the
// same query and request cursor. Only accepted pages consume the page budget.
// The caller can set a context deadline to bound requests and waits together.
func (w *BoundedWalk) NextPageWithRetry(ctx context.Context, retry PageRetry) (Page, error) {
	if retry.MaxRetries < 0 || retry.Delay < 0 {
		return Page{}, errors.New("query page retry count and delay must be nonnegative")
	}
	for retries := 0; ; retries++ {
		page, err := w.NextPage(ctx)
		if err == nil {
			return page, nil
		}
		err = withQueryCancellation(ctx, err)
		if !retryablePage(err) {
			return Page{}, err
		}
		if retries == retry.MaxRetries {
			return Page{}, fmt.Errorf("%w (max retries %d): %w", ErrPageRetryExhausted, retry.MaxRetries, err)
		}
		if err := waitPageRetry(ctx, retry.Delay); err != nil {
			return Page{}, err
		}
	}
}

func retryablePage(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, ErrPageBytes) || errors.Is(err, ErrMalformedPage) {
		return false
	}
	if errors.Is(err, ErrPageInterrupted) {
		return true
	}
	var response *HTTPError
	return errors.As(err, &response) && response.Retryable()
}

func waitPageRetry(ctx context.Context, delay time.Duration) error {
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
		case <-timer.C:
		}
	}
	return withQueryCancellation(ctx, nil)
}
