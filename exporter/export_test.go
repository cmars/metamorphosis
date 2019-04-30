// Copyright 2019 CanonicalLtd

package main

import (
	"context"
	"time"

	client "github.com/influxdata/influxdb1-client/v2"
)

func ProcessData(ctx context.Context, config TopicConfig, influxClient client.Client, data [][]byte, timestamps []time.Time) error {
	c := dataConsumer{
		config:       config,
		influxClient: influxClient,
	}
	return c.process(ctx, data, timestamps)
}
