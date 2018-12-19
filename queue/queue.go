package queue

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/eventsource-ecosystem/eventsource"
	"golang.org/x/sync/errgroup"
)

type Subscription struct {
	cancel context.CancelFunc
	group  *errgroup.Group
}

func (s *Subscription) Close() error {
	s.cancel()
	return s.group.Wait()
}

type HandlerFunc func(ctx context.Context, event eventsource.Event) error

type logFunc func(format string, args ...interface{})

type Options struct {
	printf logFunc
}

type Option func(*Options)

func WithLogger(fn func(format string, args ...interface{})) func(*Options) {
	return func(o *Options) {
		o.printf = fn
	}
}

func Subscribe(api sqsiface.SQSAPI, queueName string, serializer eventsource.Serializer, fn HandlerFunc, opts ...Option) (*Subscription, error) {
	var options = Options{
		printf: func(format string, args ...interface{}) {},
	}
	for _, opt := range opts {
		opt(&options)
	}

	options.printf("subscribing to sqs event queue, %v", queueName)

	listOut, err := api.ListQueues(&sqs.ListQueuesInput{
		QueueNamePrefix: aws.String(queueName),
	})
	if err != nil {
		return nil, fmt.Errorf("sqs.ListQueues failed - %v", err)
	}

	var queueUrl *string
	for _, item := range listOut.QueueUrls {
		if strings.HasSuffix(*item, "/"+queueName) {
			queueUrl = item
		}
	}
	if queueUrl == nil {
		return nil, fmt.Errorf("queue not found, %v", queueName)
	}

	options.printf("found %v", *queueUrl)

	var (
		received       = make(chan *sqs.Message, 10)
		completed      = make(chan *sqs.Message, 10)
		parent, cancel = context.WithCancel(context.Background())
		group, ctx     = errgroup.WithContext(parent)
		sub            = &Subscription{
			cancel: cancel,
			group:  group,
		}
	)

	group.Go(func() error {
		return receiveLoop(ctx, options.printf, api, queueUrl, received)
	})
	group.Go(func() error {
		return handleLoop(ctx, options.printf, received, completed, serializer, fn)
	})
	group.Go(func() error {
		return deleteLoop(ctx, options.printf, api, queueUrl, 15*time.Second, completed)
	})

	return sub, nil
}

func receiveLoop(ctx context.Context, printf logFunc, api sqsiface.SQSAPI, queueUrl *string, receive chan *sqs.Message) error {
	for {
		output, err := api.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
			MaxNumberOfMessages: aws.Int64(10),
			QueueUrl:            queueUrl,
			VisibilityTimeout:   aws.Int64(240),
			WaitTimeSeconds:     aws.Int64(20),
		})
		if err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(15 * time.Second):
				continue
			}
		}

		if v := len(output.Messages); v > 0 {
			printf("received %v messages", v)
		}

		for _, m := range output.Messages {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case receive <- m:
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
}

func handleMessage(ctx context.Context, printf logFunc, serializer eventsource.Serializer, fn HandlerFunc, m *sqs.Message) error {
	if m == nil || m.Body == nil {
		return nil
	}

	data, err := base64.StdEncoding.DecodeString(*m.Body)
	if err != nil {
		printf("unable to decode sqs body - %v\n", err)
		return nil
	}

	event, err := serializer.UnmarshalEvent(eventsource.Record{Data: data})
	if err != nil {
		return fmt.Errorf("unable to unmarshal event -> %v", *m.Body)
	}

	return fn(ctx, event)
}

func handleLoop(ctx context.Context, printf logFunc, received, completed chan *sqs.Message, serializer eventsource.Serializer, fn HandlerFunc) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case v := <-received:
			if err := handleMessage(ctx, printf, serializer, fn, v); err != nil {
				printf("unable to handle event - %v", err)
				return err
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case completed <- v:
			}
		}
	}
}

func deleteLoop(ctx context.Context, printf logFunc, api sqsiface.SQSAPI, queueUrl *string, interval time.Duration, completed chan *sqs.Message) error {
	input := &sqs.DeleteMessageBatchInput{
		QueueUrl: queueUrl,
	}

	deleteMessages := func() {
		if len(input.Entries) == 0 {
			return
		}

		for attempt := 1; attempt <= 3; attempt++ {
			if _, err := api.DeleteMessageBatchWithContext(ctx, input); err != nil {
				printf("delete messages failed - %v", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(15 * time.Second):
					continue
				}
			}

			printf("deleted %v messages", len(input.Entries))
			break
		}

		input.Entries = nil // reset Entries
	}
	defer deleteMessages()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-ticker.C:
			deleteMessages()

		case v := <-completed:
			input.Entries = append(input.Entries, &sqs.DeleteMessageBatchRequestEntry{
				Id:            aws.String(strconv.Itoa(len(input.Entries))),
				ReceiptHandle: v.ReceiptHandle,
			})
		}

		if len(input.Entries) == 10 {
			deleteMessages()
		}
	}
}
