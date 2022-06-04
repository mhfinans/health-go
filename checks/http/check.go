package http

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

const defaultRequestTimeout = 5 * time.Second

// Config is the HTTP checker configuration settings container.
type Config struct {
	// URL is the remote service health check URL.
	URL string
	// RequestTimeout is timeout duration
	// If not set - 5 seconds
	RequestTimeout time.Duration
	// CheckResponse is custom check for response
	CheckResponse func(r io.ReadCloser) error
}

// New creates new HTTP service health check that verifies the following:
// - connection establishing
// - getting response status from defined URL
// - verifying that status code is less than 500
// - if CheckResponse function passed, check response
func New(config Config) func(ctx context.Context) error {
	if config.RequestTimeout == 0 {
		config.RequestTimeout = defaultRequestTimeout
	}

	return func(ctx context.Context) error {
		req, err := http.NewRequest(http.MethodGet, config.URL, nil)
		if err != nil {
			return fmt.Errorf("creating the request for the health check failed: %w", err)
		}

		ctx, cancel := context.WithTimeout(ctx, config.RequestTimeout)
		defer cancel()

		// Inform remote service to close the connection after the transaction is complete
		req.Header.Set("Connection", "close")
		req = req.WithContext(ctx)

		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return fmt.Errorf("making the request for the health check failed: %w", err)
		}
		defer res.Body.Close()

		if res.StatusCode >= http.StatusInternalServerError {
			return errors.New("remote service is not available at the moment")
		}

		if config.CheckResponse != nil {
			return config.CheckResponse(res.Body)
		}

		return nil
	}
}
