package kafka

import (
	"context"
	"github.com/stretchr/testify/require"
	"os"
	"strings"
	"testing"
	"time"
)

const kafkaBootstrapEnv = "HEALTH_GO_KAFKA_BOOTSTRAP"
const kafkaVersionEnv = "HEALTH_GO_KAFKA_VERSION"

func TestNew(t *testing.T) {
	check := New(Config{
		Bootstrap:   getBootstrap(t),
		Version:     getVersion(t),
		ServiceName: "test-service",
		Timeout:     30 * time.Second,
	})

	err := check(context.Background())
	require.NoError(t, err)
}

func TestTimeout(t *testing.T) {
	check := New(Config{
		Bootstrap:   []string{"google.com:9092"},
		Version:     "2.8.0",
		ServiceName: "test-timeout",
		Timeout:     3 * time.Second,
	})

	before := time.Now()

	err := check(context.Background())

	require.Error(t, err)
	require.Less(t, time.Now().Unix()-before.Unix(), int64(4))
}

func TestRandStr(t *testing.T) {
	str := randStr(10)
	require.Len(t, str, 10)

	str = randStr(1)
	require.Len(t, str, 1)

	str = randStr(100)
	require.Len(t, str, 100)
}

func getBootstrap(t *testing.T) []string {
	t.Helper()

	kafkaString, ok := os.LookupEnv(kafkaBootstrapEnv)
	require.True(t, ok)

	kafkaBootstrap := strings.Split(kafkaString, ",")

	return kafkaBootstrap
}

func getVersion(t *testing.T) string {
	t.Helper()

	kafkaVersion, ok := os.LookupEnv(kafkaVersionEnv)
	require.True(t, ok)

	return kafkaVersion
}
