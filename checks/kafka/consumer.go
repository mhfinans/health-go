package kafka

import (
	"context"
	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	"log"
	"sync"
)

type Server interface {
	Listen(wg *sync.WaitGroup, close <-chan struct{})
	Close() error
	GetConsumer() sarama.ConsumerGroup
}

type Consumer interface {
	AddClaimer(string, MessageClaimer)
	AddSetupHook(hook Hook)
	AddCleanupHook(hook Hook)
}

type MessageClaimer func(*sarama.ConsumerMessage) bool

type consumer struct {
	consumerGroup       sarama.ConsumerGroup
	topics              []string
	logIncomingMessages bool
	claimers            map[string]MessageClaimer
	cleanupHooks        []Hook
	setupHooks          []Hook
	ready               chan bool
	errorChan           chan error
}

type Hook func(session sarama.ConsumerGroupSession)

func (c *consumer) Listen(parentWg *sync.WaitGroup, close <-chan struct{}) {
	defer parentWg.Done()

	if c.errorChan != nil {
		go c.trackErrors()
	}

	ctx, cancel := context.WithCancel(context.Background())

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go c.createConsumerSession(ctx, wg)

	<-c.ready // Wait till the consumer has been set up
	log.Println("Sarama consumer up and running!...")

	select {
	case <-ctx.Done():
		log.Println("terminating: context cancelled")
	case <-close:
		log.Println("terminating: close channel signal")
	}

	cancel()
	wg.Wait()

	if err := c.consumerGroup.Close(); err != nil {
		log.Panicf("Error closing kafka consumer: %v", err)
	}
}

func (c *consumer) AddCleanupHook(hook Hook) {
	c.cleanupHooks = append(c.cleanupHooks, hook)
}

func (c *consumer) AddSetupHook(hook Hook) {
	c.setupHooks = append(c.setupHooks, hook)
}

func (c *consumer) AddClaimer(topic string, claimer MessageClaimer) {
	c.claimers[topic] = claimer
}

func (c *consumer) Setup(session sarama.ConsumerGroupSession) error {
	log.Println("setting up sarama consumer group...")
	for _, h := range c.setupHooks {
		h(session)
	}
	close(c.ready)
	return nil
}

func (c *consumer) Cleanup(session sarama.ConsumerGroupSession) error {
	log.Println("cleaning up sarama consumer group...")
	for _, h := range c.cleanupHooks {
		h(session)
	}
	return nil
}

func (c *consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			{
				if !ok {
					return nil
				}

				claimer, exists := c.claimers[message.Topic]

				if !exists {
					log.Println("no handler exists for this topic", message.Topic)
					continue
				}

				canProcess := claimer(message)
				if !canProcess {
					log.Println("could not process message", string(message.Value))
					continue
				}

				session.MarkMessage(message, "")
				session.Commit()
			}

		case <-session.Context().Done():
			{
				return nil
			}
		}
	}
}

func (c *consumer) Close() error {
	return c.consumerGroup.Close()
}

func (c *consumer) GetConsumer() sarama.ConsumerGroup {
	return c.consumerGroup
}

func (c *consumer) createConsumerSession(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		if err := c.consumerGroup.Consume(ctx, c.topics, c); err != nil {
			log.Panicf("error from consumer: %v\n", err)
		}

		if ctx.Err() != nil {
			return
		}

		c.ready = make(chan bool)
	}
}

func (c *consumer) trackErrors() {
	for err := range c.consumerGroup.Errors() {
		log.Println("error in consumer group", zap.Error(err))
		c.errorChan <- err
	}
}
