package http

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const httpURLEnv = "HEALTH_GO_HTTP_URL"

func TestNew(t *testing.T) {
	check := New(Config{
		URL: getURL(t),
	})

	err := check(context.Background())
	require.NoError(t, err)
}

func TestCheckResponseNoError(t *testing.T) {
	check := New(Config{
		URL: getURL(t),
		CheckResponse: func(r io.ReadCloser) error {
			return nil
		},
	})

	err := check(context.Background())
	require.NoError(t, err)
}

func TestCheckResponseError(t *testing.T) {
	check := New(Config{
		URL: getURL(t),
		CheckResponse: func(r io.ReadCloser) error {
			return errors.New("response is not healthy")
		},
	})

	err := check(context.Background())
	require.Error(t, err)
}

func getURL(t *testing.T) string {
	t.Helper()

	httpURL, ok := os.LookupEnv(httpURLEnv)
	require.True(t, ok)

	// "docker-compose port <service> <port>" returns 0.0.0.0:XXXX locally, change it to local port
	httpURL = strings.Replace(httpURL, "0.0.0.0:", "127.0.0.1:", 1)

	return httpURL
}
