package s3firehose

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/eventsource-ecosystem/eventsource"
)

type HandlerFunc func(ctx context.Context, event eventsource.Event) error

func handleObject(ctx context.Context, api s3iface.S3API, serializer eventsource.Serializer, fn HandlerFunc, bucket, key string) error {
	input := s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	output, err := api.GetObjectWithContext(ctx, &input)
	if err != nil {
		return err
	}
	defer output.Body.Close()

	s := bufio.NewScanner(output.Body)
	for line := 1; s.Scan(); line++ {
		data, err := base64.StdEncoding.DecodeString(s.Text())
		if err != nil {
			return fmt.Errorf("unable to base64 decode line %v - %v", line, err)
		}

		event, err := serializer.UnmarshalEvent(eventsource.Record{Data: data})
		if err != nil {
			return fmt.Errorf("unable to unmarshal event - %v", err)
		}

		if err := fn(ctx, event); err != nil {
			return err
		}
	}

	return nil
}

func Replay(ctx context.Context, api s3iface.S3API, serializer eventsource.Serializer, fn HandlerFunc, bucket, prefix string) error {
	var token *string

	for {
		input := s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			ContinuationToken: token,
			Prefix:            aws.String(prefix),
		}

		output, err := api.ListObjectsV2WithContext(ctx, &input)
		if err != nil {
			return fmt.Errorf("unable to list objects from s3://%v/%v - %v", bucket, prefix, err)
		}

		for _, item := range output.Contents {
			if err := handleObject(ctx, api, serializer, fn, bucket, *item.Key); err != nil {
				return fmt.Errorf("unable to process s3 object, s3://%v/%v - %v", bucket, *item.Key, err)
			}
		}

		token = output.NextContinuationToken
		if token == nil {
			break
		}
	}

	return nil
}
