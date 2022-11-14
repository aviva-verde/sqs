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

type SendFunc func(ctx context.Context, v any) (err error)
type SendBatchFunc func(ctx context.Context, v []any) (err error)

// NewSender creates a Sender using the default configuration.
func NewSender(ctx context.Context, queueURL string) (s Sender, err error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return
	}
	return NewSenderFromConfig(cfg, queueURL)
}

// NewSenderFromConfig creates a Sender using the provided configuration.
func NewSenderFromConfig(cfg aws.Config, queueURL string, optFns ...func(*sqs.Options)) (s Sender, err error) {
	s = Sender{
		client:   sqs.NewFromConfig(cfg, optFns...),
		queueURL: queueURL,
	}
	return
}

// Sender sends messages to SQS in JSON format.
type Sender struct {
	client   *sqs.Client
	queueURL string
}

// Send a message to SQS.
func (s Sender) Send(ctx context.Context, v any) (err error) {
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
func (s Sender) SendBatch(ctx context.Context, v []any) (err error) {
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
