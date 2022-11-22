package kafka

import (
	"context"
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/mhfinans/utility/v1/kafka"
	"math/rand"
	"sync"
	"time"
)

// Config is the Kafka checker configuration settings container.
type Config struct {
	// Bootstrap of Kafka cluster. Required.
	Bootstrap []string
	// Version of Kafka
	Version string
	// ServiceName name of service which is implementing health check
	ServiceName string
	// Timeout of a cycle of produce and consume
	Timeout time.Duration
	// CustomTopicName optional custom topic name
	CustomTopicName string
}

// New creates new Kafka health check that verifies the following:
// - connection establishing
// - publishing a ping message and verifying the response
func New(config Config) func(ctx context.Context) error {
	var topic string
	if config.CustomTopicName != "" {
		topic = config.CustomTopicName
	} else {
		topic = fmt.Sprintf("%s.health-check-topic", config.ServiceName)
	}

	groupId := fmt.Sprintf("%s.health-check-consumer-group", config.ServiceName)
	rand.Seed(time.Now().UnixNano())

	return func(ctx context.Context) error {
		deadline := time.Now().Add(config.Timeout)

		kErr := make(chan error)
		message := randStr(10)

		k := kafka.NewKafka(kafka.Config{
			Bootstrap: config.Bootstrap,
			Version:   config.Version,
		})

		cConf := k.NewConsumerDefaultConfig()
		cConf.Metadata.Timeout = config.Timeout
		cConf.Net.WriteTimeout = config.Timeout
		cConf.Net.DialTimeout = config.Timeout
		cConf.Net.ReadTimeout = config.Timeout
		cConf.Admin.Timeout = config.Timeout
		cConf.Consumer.Offsets.Initial = sarama.OffsetNewest

		admin, err := sarama.NewClusterAdmin(config.Bootstrap, cConf)
		if err != nil {
			return fmt.Errorf("cannot create cluster admin %w", err)
		}

		cg, err := k.NewConsumerWithConfig(groupId, cConf)
		if err != nil {
			return fmt.Errorf("cannot create consumer %w", err)
		}

		s, c, err := kafka.NewSimplePrebuiltConsumer(k, kafka.SimpleConsumerConfig{
			SimplePrebuiltConsumerConfig: kafka.SimplePrebuiltConsumerConfig{
				Topics:              []string{topic},
				LogIncomingMessages: false,
				Logger:              nil,
				ErrorChannel:        nil,
			},
		}, cg)
		if err != nil {
			return fmt.Errorf("cannot create consumer %w", err)
		}

		c.AddClaimer(topic, func(msg *sarama.ConsumerMessage) bool {
			if time.Now().Before(deadline) {
				value := string(msg.Value)
				if value == message {
					kErr <- nil
				}
			} else {
				kErr <- errors.New("could not get sent message")
			}
			return true
		})

		c.AddCleanupHook(func(session sarama.ConsumerGroupSession) {
			fmt.Println("running cleanup hook")

			err := admin.DeleteTopic(topic)
			if err != nil {
				fmt.Printf("could not delete generated topic %s\n", err.Error())
			}

			err = admin.Close()
			if err != nil {
				fmt.Printf("could not close cluster admin connection. it can lead to goroutine leak. %s\n", err.Error())
			}
		})

		closeChan := make(chan struct{})

		wg := sync.WaitGroup{}
		wg.Add(1)
		go s.Listen(&wg, closeChan)

		p, err := k.NewSyncProducer()
		if err != nil {
			return fmt.Errorf("cannot create kafka producer %w", err)
		}
		defer func(p sarama.SyncProducer) {
			err := p.Close()
			if err != nil {
				fmt.Println("could not clean up producer in health check", err)
			}
		}(p)

		_, _, err = p.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(message),
		})
		if err != nil {
			return fmt.Errorf("cannot produce message to kafka topic %w", err)
		}

		result := <-kErr
		close(kErr)
		close(closeChan)
		return result
	}
}

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randStr(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
