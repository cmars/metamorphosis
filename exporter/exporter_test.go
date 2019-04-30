// Copyright 2019 CanonicalLtd

package main_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	client "github.com/influxdata/influxdb1-client/v2"

	exporter "github.com/cloud-green/metamorphosis/exporter"
)

func TestConsumer(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		about         string
		config        exporter.TopicConfig
		data          map[string]interface{}
		timestamps    []time.Time
		expectedError string
		assertBatches func(*qt.C, client.BatchPoints)
	}{{
		about: "a histogram test",
		config: exporter.TopicConfig{
			Topic: "test-topic",
			Type:  "histogram",
		},
		data: map[string]interface{}{
			"0":  1,
			"10": 20,
			"20": 5,
		},
		timestamps: []time.Time{
			time.Date(2019, 5, 1, 12, 0, 0, 0, time.UTC),
		},
		assertBatches: func(c *qt.C, points client.BatchPoints) {
			p := points.Points()
			c.Assert(p, qt.HasLen, 1)
			point := p[0]
			c.Assert(point.String(), qt.Equals, fmt.Sprintf("test-topic 0=1,10=20,20=5 1556712000000000000"))
		},
	}, {
		about: "a histogram test - with padding",
		config: exporter.TopicConfig{
			Topic:     "test-topic",
			Type:      "histogram",
			KeyFormat: "%04d",
		},
		data: map[string]interface{}{
			"0":  1,
			"10": 20,
			"20": 5,
		},
		timestamps: []time.Time{
			time.Date(2019, 5, 1, 12, 0, 0, 0, time.UTC),
		},
		assertBatches: func(c *qt.C, points client.BatchPoints) {
			p := points.Points()
			c.Assert(p, qt.HasLen, 1)
			point := p[0]
			c.Assert(point.String(), qt.Equals, fmt.Sprintf("test-topic 0000=1,0010=20,0020=5 1556712000000000000"))
		},
	}, {
		about: "top-k",
		config: exporter.TopicConfig{
			Topic: "test-topic",
			Type:  "top-k",
		},
		data: map[string]interface{}{
			"a": 1,
			"b": 20,
			"c": 5,
		},
		timestamps: []time.Time{
			time.Date(2019, 5, 1, 12, 0, 0, 0, time.UTC),
		},
		assertBatches: func(c *qt.C, points client.BatchPoints) {
			p := points.Points()
			c.Assert(p, qt.HasLen, 1)
			point := p[0]
			c.Assert(point.String(), qt.Equals, fmt.Sprintf("test-topic a=1,b=20,c=5 1556712000000000000"))
		},
	}, {
		about: "fields",
		config: exporter.TopicConfig{
			Topic: "test-topic",
			Fields: map[string]string{
				"a": "number",
				"b": "string",
				"d": "number",
			},
		},
		data: map[string]interface{}{
			"a": 42,
			"b": "just a string",
			"c": 5,
		},
		timestamps: []time.Time{
			time.Date(2019, 5, 1, 12, 0, 0, 0, time.UTC),
		},
		assertBatches: func(c *qt.C, points client.BatchPoints) {
			p := points.Points()
			c.Assert(p, qt.HasLen, 1)
			point := p[0]
			c.Assert(point.String(), qt.Equals, fmt.Sprintf(`test-topic a=42,b="just a string" 1556712000000000000`))
		},
	}}

	for i, test := range tests {
		c.Logf("running test %d: %s", i, test.about)

		influxClient := newTestInfluxClient()

		data, err := json.Marshal(test.data)
		c.Assert(err, qt.IsNil)

		err = exporter.ProcessData(context.Background(), test.config, influxClient, [][]byte{data}, test.timestamps)
		if test.expectedError != "" {
			c.Assert(err, qt.ErrorMatches, test.expectedError)
		} else {
			c.Assert(err, qt.IsNil)
			test.assertBatches(c, influxClient.bp)
		}
	}

}

func newTestInfluxClient() *testInfluxClient {
	return &testInfluxClient{}
}

type testInfluxClient struct {
	client.Client

	bp client.BatchPoints
}

func (c *testInfluxClient) Write(bp client.BatchPoints) error {
	c.bp = bp
	return nil
}
