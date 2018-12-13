package s3firehose

import (
	"bytes"
	"context"
	"encoding/base64"
	"io/ioutil"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/eventsource-ecosystem/eventsource"
)

type Sample struct {
	eventsource.Model
	Name string
}

type Mock struct {
	s3iface.S3API

	serializer eventsource.Serializer
	events     []eventsource.Event
}

func (m *Mock) ListObjectsV2WithContext(ctx aws.Context, input *s3.ListObjectsV2Input, opts ...request.Option) (*s3.ListObjectsV2Output, error) {
	return &s3.ListObjectsV2Output{
		Contents: []*s3.Object{
			{
				Key: aws.String("blah"),
			},
		},
	}, nil
}

func (m *Mock) GetObjectWithContext(ctx aws.Context, inputs *s3.GetObjectInput, opts ...request.Option) (*s3.GetObjectOutput, error) {
	buf := bytes.NewBuffer(nil)
	for _, event := range m.events {
		record, err := m.serializer.MarshalEvent(event)
		if err != nil {
			return nil, err
		}

		buf.WriteString(base64.StdEncoding.EncodeToString(record.Data))
		buf.WriteString("\n")
	}

	return &s3.GetObjectOutput{
		Body: ioutil.NopCloser(buf),
	}, nil
}

func TestReplay(t *testing.T) {
	var (
		ctx        = context.Background()
		serializer = eventsource.NewJSONSerializer(Sample{})
		event      = Sample{
			Model: eventsource.Model{ID: "abc"},
			Name:  "blah",
		}
		api = &Mock{
			events:     []eventsource.Event{event},
			serializer: serializer,
		}
		events []eventsource.Event
		fn     = func(ctx context.Context, event eventsource.Event) error {
			events = append(events, event)
			return nil
		}
	)

	err := Replay(ctx, api, serializer, fn, "vavende-events-dev", "master-events--integration/")
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	if got, want := len(events), 1; got != want {
		t.Fatalf("got %v; want %v", got, want)
	}
}
