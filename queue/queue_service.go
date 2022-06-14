package queue

import (
	"github.com/nsqio/go-nsq"
	"github.com/tereus-project/tereus-go-std/queue/internal"
)

type QueueService struct {
	nsqService *internal.NSQService
}

func NewQueueService(nsqdEndpoint string, nsqlookupdEndpoint string) (*QueueService, error) {
	nsqService, err := internal.NewNSQService(nsqdEndpoint, nsqlookupdEndpoint)
	if err != nil {
		return nil, err
	}

	return &QueueService{
		nsqService: nsqService,
	}, nil
}

func (s *QueueService) Publish(topic string, message []byte) error {
	return s.nsqService.Publish(topic, message)
}

func (s *QueueService) AddHandler(topic string, channel string, handler func(m *nsq.Message) error) {
	s.nsqService.AddHandler(topic, channel, handler)
}

func (s *QueueService) Close() {
	s.nsqService.ShutDown()
}
