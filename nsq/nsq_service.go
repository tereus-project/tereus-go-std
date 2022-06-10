package services

import (
	"runtime"
	"sync"

	"github.com/nsqio/go-nsq"
)

type NSQMessageHandler interface {
	HandleMessage(m *nsq.Message) error
}

type NSQService struct {
	config             *nsq.Config
	nsqdEndpoint       string
	nsqlookupdEndpoint string

	consumers      []*nsq.Consumer
	consumersMutex sync.RWMutex

	producer *nsq.Producer
}

func NewNSQService(nsqdEndpoint string, nsqlookupdEndpoint string) (*NSQService, error) {
	NSQConfig := nsq.NewConfig()

	producer, err := nsq.NewProducer(nsqdEndpoint, NSQConfig)
	if err != nil {
		return nil, err
	}

	return &NSQService{
		config:             NSQConfig,
		nsqdEndpoint:       nsqdEndpoint,
		producer:           producer,
		nsqlookupdEndpoint: nsqlookupdEndpoint,
	}, nil
}

func (s *NSQService) Publish(topic string, message []byte) error {
	return s.producer.Publish(topic, message)
}

func (s *NSQService) RegisterHandler(topic string, channel string, handler NSQMessageHandler) error {
	consumer, err := nsq.NewConsumer(topic, channel, s.config)
	if err != nil {
		return err
	}

	consumer.AddConcurrentHandlers(nsq.HandlerFunc(func(m *nsq.Message) error {
		if len(m.Body) == 0 {
			// Returning nil will automatically send a FIN command to NSQ to mark the message as processed.
			// In this case, a message with an empty body is simply ignored/discarded.
			return nil
		}
		return handler.HandleMessage(m)
	}), runtime.NumCPU())

	// Use nsqlookupd to discover nsqd instances.
	err = consumer.ConnectToNSQLookupd(s.nsqlookupdEndpoint)
	if err != nil {
		return err
	}

	s.consumersMutex.Lock()
	s.consumers = append(s.consumers, consumer)
	s.consumersMutex.Unlock()

	return nil
}

func (s *NSQService) ShutDown() {
	// Gracefully stop all consumers
	s.consumersMutex.RLock()
	defer s.consumersMutex.RUnlock()
	for _, consumer := range s.consumers {
		consumer.Stop()
	}

	// Gracefully producer
	s.producer.Stop()
}
