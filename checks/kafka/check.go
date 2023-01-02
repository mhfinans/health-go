package kafka

import (
	"context"
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
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
		kConfig := sarama.NewConfig()

		version, err := sarama.ParseKafkaVersion(config.Version)
		if err != nil {
			log.Fatalln(err)
		}

		kConfig.Version = version
		kConfig.Metadata.Timeout = config.Timeout
		kConfig.Net.WriteTimeout = config.Timeout
		kConfig.Net.DialTimeout = config.Timeout
		kConfig.Net.ReadTimeout = config.Timeout
		kConfig.Admin.Timeout = config.Timeout

		kConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
		kConfig.Consumer.Offsets.AutoCommit.Enable = false

		kConfig.Producer.RequiredAcks = sarama.WaitForAll
		kConfig.Producer.Compression = sarama.CompressionSnappy
		kConfig.Producer.Partitioner = sarama.NewRoundRobinPartitioner
		kConfig.Producer.Retry.Max = 10
		kConfig.Producer.Flush.Frequency = 500 * time.Millisecond
		kConfig.Producer.Return.Successes = true
		kConfig.Producer.Return.Errors = true

		cg, err := sarama.NewConsumerGroup(config.Bootstrap, groupId, kConfig)
		if err != nil {
			return fmt.Errorf("cannot create consumer %w", err)
		}
		defer func(cg sarama.ConsumerGroup) {
			err := cg.Close()
			if err != nil {
				fmt.Println("could not close consumer group:", err)
			}
		}(cg)

		c := &consumer{
			consumerGroup:       cg,
			topics:              []string{topic},
			logIncomingMessages: false,
			claimers:            make(map[string]MessageClaimer),
			cleanupHooks:        make([]Hook, 0),
			setupHooks:          make([]Hook, 0),
			ready:               make(chan bool),
			errorChan:           nil,
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

		closeChan := make(chan struct{})

		wg := sync.WaitGroup{}
		wg.Add(1)
		go c.Listen(&wg, closeChan)

		p, err := sarama.NewSyncProducer(config.Bootstrap, kConfig)
		if err != nil {
			return err
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
