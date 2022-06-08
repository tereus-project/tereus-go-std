package kafka

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

type KafkaService struct {
	endpoint        string
	consumerGroupID string
	writers         map[string]*kafka.Writer
}

func NewKafkaService(endpoint string, consumerGroupID string) (*KafkaService, error) {
	return &KafkaService{
		endpoint:        endpoint,
		consumerGroupID: consumerGroupID,
		writers:         make(map[string]*kafka.Writer),
	}, nil
}

func (s *KafkaService) getWriterForTopic(topicName string) (*kafka.Writer, error) {
	w, ok := s.writers[topicName]
	if !ok {
		w := &kafka.Writer{
			Addr:                   kafka.TCP(s.endpoint),
			Topic:                  topicName,
			Balancer:               &kafka.LeastBytes{},
			AllowAutoTopicCreation: true,
		}
		s.writers[topicName] = w

		return w, nil
	}

	return w, nil
}

func (s *KafkaService) CloseAllWriters() {
	for _, w := range s.writers {
		err := w.Close()
		if err != nil {
			logrus.WithError(err).Error("Failed to close kafka writer")
		}
	}
}

func (s *KafkaService) Publish(topic string, key string, messageBytes []byte) error {
	writer, err := s.getWriterForTopic(topic)
	if err != nil {
		return err
	}

	return writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(key),
			Value: messageBytes,
		},
	)
}

func (s *KafkaService) Consume(topic string) <-chan []byte {
	ch := make(chan []byte)

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{s.endpoint},
		GroupID:     s.consumerGroupID,
		Topic:       topic,
		MaxWait:     1 * time.Second,
		StartOffset: kafka.LastOffset,
	})

	go func() {
		defer r.Close()
		defer close(ch)

		for {
			msg, err := r.ReadMessage(context.Background())
			if err != nil {
				logrus.WithError(err).Error("Error reading message")
			}

			ch <- msg.Value
		}
	}()

	return ch
}
