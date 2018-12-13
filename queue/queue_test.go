package queue

import (
	"context"
	"encoding/base64"
	"log"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/eventsource-ecosystem/eventsource"
)

type Mock struct {
	sqsiface.SQSAPI

	events     []eventsource.Event
	serializer *eventsource.JSONSerializer
}

func (m *Mock) ListQueues(input *sqs.ListQueuesInput) (*sqs.ListQueuesOutput, error) {
	return &sqs.ListQueuesOutput{
		QueueUrls: []*string{
			aws.String("/" + *input.QueueNamePrefix),
		},
	}, nil
}

func (m *Mock) ReceiveMessageWithContext(ctx aws.Context, input *sqs.ReceiveMessageInput, opts ...request.Option) (*sqs.ReceiveMessageOutput, error) {
	if len(m.events) == 0 {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	event := m.events[0]
	m.events = m.events[1:]
	record, err := m.serializer.MarshalEvent(event)
	if err != nil {
		return nil, err
	}

	return &sqs.ReceiveMessageOutput{
		Messages: []*sqs.Message{
			{
				Body: aws.String(base64.StdEncoding.EncodeToString(record.Data)),
			},
		},
	}, nil
}

func (m *Mock) DeleteMessageBatchWithContext(aws.Context, *sqs.DeleteMessageBatchInput, ...request.Option) (*sqs.DeleteMessageBatchOutput, error) {
	return nil, nil
}

type Sample struct {
	eventsource.Model
	Name string
}

func TestSubscription(t *testing.T) {
	var (
		queueName  = "blah"
		serializer = eventsource.NewJSONSerializer(Sample{})
		done       = make(chan struct{})
		event      = Sample{
			Model: eventsource.Model{ID: "abc"},
			Name:  "blah",
		}
		api = &Mock{
			events:     []eventsource.Event{event},
			serializer: serializer,
		}
		fn = func(ctx context.Context, event eventsource.Event) error {
			defer close(done)
			return nil
		}
	)

	sub, err := Subscribe(api, queueName, serializer, fn, WithLogger(log.Printf))
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}
	defer sub.Close()

	<-done
}
