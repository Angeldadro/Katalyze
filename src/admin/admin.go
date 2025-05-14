package admin

import (
	"context"
	"fmt"

	"github.com/Angeldadro/Katalyze/src/types"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type KafkaAdminClient struct {
	options          []types.Option
	kafkaAdminClient *kafka.AdminClient
}

func NewKafkaAdminClient(adminClient *kafka.AdminClient, options []types.Option) (*KafkaAdminClient, error) {
	if adminClient == nil {
		return nil, fmt.Errorf("adminClient is nil")
	}
	if options == nil {
		options = []types.Option{}
	}
	return &KafkaAdminClient{
		options:          options,
		kafkaAdminClient: adminClient,
	}, nil
}
func (a *KafkaAdminClient) CreateTopic(topic string, numPartitions, replicationFactor int) error {
	topicSpec := kafka.TopicSpecification{
		Topic:             topic,
		NumPartitions:     numPartitions,
		ReplicationFactor: replicationFactor,
	}
	result, err := a.kafkaAdminClient.CreateTopics(
		context.Background(),
		[]kafka.TopicSpecification{topicSpec},
	)
	if err != nil {
		return err
	}
	if result[0].Error.Code() != kafka.ErrNoError {
		return result[0].Error
	}
	return nil
}

func (a *KafkaAdminClient) Options() []types.Option {
	return a.options
}
func (a *KafkaAdminClient) DeleteTopic(topic string) error {
	result, err := a.kafkaAdminClient.DeleteTopics(
		context.Background(),
		[]string{topic},
	)
	if err != nil {
		return err
	}
	if result[0].Error.Code() != kafka.ErrNoError {
		return result[0].Error
	}
	return nil
}

func (a *KafkaAdminClient) ListTopics() ([]string, error) {
	metadata, err := a.kafkaAdminClient.GetMetadata(nil, true, 10000)
	if err != nil {
		return nil, err
	}
	topics := make([]string, 0, len(metadata.Topics))
	for topic := range metadata.Topics {
		topics = append(topics, topic)
	}
	return topics, nil
}

func (a *KafkaAdminClient) Close() error {
	a.kafkaAdminClient.Close()
	return nil
}
