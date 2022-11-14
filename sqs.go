package sqs

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// NewSender creates a Sender using the default configuration.
func NewSender[T any](ctx context.Context, queueURL string) (s Sender[T], err error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return
	}
	return NewSenderFromConfig[T](cfg, queueURL)
}

// NewSenderFromConfig creates a Sender using the provided configuration.
func NewSenderFromConfig[T any](cfg aws.Config, queueURL string, optFns ...func(*sqs.Options)) (s Sender[T], err error) {
	s = Sender[T]{
		client:   sqs.NewFromConfig(cfg, optFns...),
		queueURL: queueURL,
	}
	return
}

// Sender sends messages to SQS in JSON format.
type Sender[T any] struct {
	client   *sqs.Client
	queueURL string
}

// Send a message to SQS.
func (s Sender[T]) Send(ctx context.Context, v T) (err error) {
	jsonValue, err := json.Marshal(v)
	if err != nil {
		return
	}
	message := &sqs.SendMessageInput{
		MessageBody: aws.String(string(jsonValue)),
		QueueUrl:    &s.queueURL,
	}
	_, err = s.client.SendMessage(ctx, message)
	return
}

// SendBatch sends a batch of messages to SQS.
func (s Sender[T]) SendBatch(ctx context.Context, v []T) (err error) {
	if len(v) == 0 {
		return nil
	}
	entries := make([]types.SendMessageBatchRequestEntry, len(v))
	for i, item := range v {
		body, err := json.Marshal(item)
		if err != nil {
			return fmt.Errorf("failed to convert to json: %w", err)
		}
		entries[i] = types.SendMessageBatchRequestEntry{
			Id:          aws.String(strconv.FormatInt(int64(i), 10)),
			MessageBody: aws.String(string(body)),
		}
	}
	_, err = s.client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
		Entries:  entries,
		QueueUrl: &s.queueURL,
	})
	return
}
