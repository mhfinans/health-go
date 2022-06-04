package main

import (
	"context"
	"errors"
	"github.com/mhfinans/health-go"
	healthHttp "github.com/mhfinans/health-go/checks/http"
	healthKafka "github.com/mhfinans/health-go/checks/kafka"
	healthMysql "github.com/mhfinans/health-go/checks/mysql"
	healthPg "github.com/mhfinans/health-go/checks/postgres"
	"net/http"
	"time"
)

func main() {
	h, _ := health.New()
	// custom kafka check example
	h.Register(health.Config{
		Name:      "kafka",
		Timeout:   time.Second * 60,
		SkipOnErr: false,
		Check: healthKafka.New(healthKafka.Config{
			Bootstrap:   []string{"127.0.0.1:9092"},
			Version:     "2.8.0",
			ServiceName: "test",
			Timeout:     time.Second * 60,
		}),
	})

	// custom health check example (fail)
	h.Register(health.Config{
		Name:      "some-custom-check-fail",
		Timeout:   time.Second * 5,
		SkipOnErr: true,
		Check:     func(context.Context) error { return errors.New("failed during custom health check") },
	})

	// custom health check example (success)
	h.Register(health.Config{
		Name:  "some-custom-check-success",
		Check: func(context.Context) error { return nil },
	})

	// http health check example
	h.Register(health.Config{
		Name:      "http-check",
		Timeout:   time.Second * 5,
		SkipOnErr: true,
		Check: healthHttp.New(healthHttp.Config{
			URL: `http://example.com`,
		}),
	})

	// postgres health check example
	h.Register(health.Config{
		Name:      "postgres-check",
		Timeout:   time.Second * 5,
		SkipOnErr: true,
		Check: healthPg.New(healthPg.Config{
			DSN: `postgres://test:test@0.0.0.0:32783/test?sslmode=disable`,
		}),
	})

	//// mysql health check example
	h.Register(health.Config{
		Name:      "mysql-check",
		Timeout:   time.Second * 5,
		SkipOnErr: true,
		Check: healthMysql.New(healthMysql.Config{
			DSN: `test:test@tcp(0.0.0.0:32778)/test?charset=utf8`,
		}),
	})

	// rabbitmq liveness test example.
	// Use it if your app has access to RabbitMQ management API.
	// This endpoint declares a test queue, then publishes and consumes a message. Intended for use by monitoring tools. If everything is working correctly, will return HTTP status 200.
	// As the default virtual host is called "/", this will need to be encoded as "%2f".
	h.Register(health.Config{
		Name:      "rabbit-aliveness-check",
		Timeout:   time.Second * 5,
		SkipOnErr: true,
		Check: healthHttp.New(healthHttp.Config{
			URL: `http://guest:guest@0.0.0.0:32780/api/aliveness-test/%2f`,
		}),
	})

	http.Handle("/liveness", h.LivenessHandler())
	http.Handle("/readiness", h.ReadinessHandler())
	http.ListenAndServe(":3000", nil)
}
