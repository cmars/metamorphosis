// Copyright 2019 CanonicalLtd

package main_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
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
		about: "top-n-not-full",
		config: exporter.TopicConfig{
			Topic:  "test-topic",
			Type:   "top-n",
			Number: 10,
		},
		data: map[string]interface{}{
			"a": 1,
			"b": 20,
			"c": 5,
		},
		timestamps: []time.Time{
			time.Date(2019, 10, 16, 16, 27, 30, 0, time.UTC),
		},
		assertBatches: func(c *qt.C, points client.BatchPoints) {
			p := points.Points()
			c.Assert(p, qt.HasLen, 10)
			c.Assert(p[0].String(), qt.Equals, fmt.Sprintf(`test-topic,top=1 name="b",value=20 1571243250000000000`))
			c.Assert(p[1].String(), qt.Equals, fmt.Sprintf(`test-topic,top=2 name="c",value=5 1571243250000000000`))
			c.Assert(p[2].String(), qt.Equals, fmt.Sprintf(`test-topic,top=3 name="a",value=1 1571243250000000000`))
			for i := 3; i < 10; i++ {
				c.Assert(p[i].String(), qt.Equals, fmt.Sprintf(`test-topic,top=%d name="",value=0 1571243250000000000`, i+1))
			}
		},
	}, {
		about: "top-n-full-with-tie",
		config: exporter.TopicConfig{
			Topic:  "test-topic",
			Type:   "top-n",
			Number: 4,
		},
		data: map[string]interface{}{
			"d": 1,
			"a": 1,
			"b": 20,
			"c": 5,
		},
		timestamps: []time.Time{
			time.Date(2019, 10, 16, 16, 27, 30, 0, time.UTC),
		},
		assertBatches: func(c *qt.C, points client.BatchPoints) {
			p := points.Points()
			c.Assert(p, qt.HasLen, 4)
			c.Assert(p[0].String(), qt.Equals, fmt.Sprintf(`test-topic,top=1 name="b",value=20 1571243250000000000`))
			c.Assert(p[1].String(), qt.Equals, fmt.Sprintf(`test-topic,top=2 name="c",value=5 1571243250000000000`))

			// In case of a tie, the order is undefined (and it doesn't really matter)
			if p[2].String() == `test-topic,top=3 name="a",value=1 1571243250000000000` {
				c.Assert(p[3].String(), qt.Equals, fmt.Sprintf(`test-topic,top=4 name="d",value=1 1571243250000000000`))
			} else if p[2].String() == `test-topic,top=3 name="d",value=1 1571243250000000000` {
				c.Assert(p[3].String(), qt.Equals, fmt.Sprintf(`test-topic,top=4 name="a",value=1 1571243250000000000`))
			} else {
				c.Log("Failing test: expected a/d entries to be in top-3/4")
				c.Fail()
			}
		},
	}, {
		about: "top-n-more-than-full",
		config: exporter.TopicConfig{
			Topic:  "test-topic",
			Type:   "top-n",
			Number: 4,
		},
		data: map[string]interface{}{
			"a": 1,
			"b": 20,
			"c": 5,
			"d": 1,
			"e": 7,
			"f": 4,
		},
		timestamps: []time.Time{
			time.Date(2019, 10, 16, 16, 27, 30, 0, time.UTC),
		},
		assertBatches: func(c *qt.C, points client.BatchPoints) {
			p := points.Points()
			c.Assert(p, qt.HasLen, 4)
			c.Assert(p[0].String(), qt.Equals, fmt.Sprintf(`test-topic,top=1 name="b",value=20 1571243250000000000`))
			c.Assert(p[1].String(), qt.Equals, fmt.Sprintf(`test-topic,top=2 name="e",value=7 1571243250000000000`))
			c.Assert(p[2].String(), qt.Equals, fmt.Sprintf(`test-topic,top=3 name="c",value=5 1571243250000000000`))
			c.Assert(p[3].String(), qt.Equals, fmt.Sprintf(`test-topic,top=4 name="f",value=4 1571243250000000000`))
		},
	}, {
		about: "top-n-empty",
		config: exporter.TopicConfig{
			Topic:  "test-topic",
			Type:   "top-n",
			Number: 100,
		},
		data: map[string]interface{}{},
		timestamps: []time.Time{
			time.Date(2019, 10, 16, 16, 27, 30, 0, time.UTC),
		},
		assertBatches: func(c *qt.C, points client.BatchPoints) {
			p := points.Points()
			c.Assert(p, qt.HasLen, 100)
			for i := 0; i < 100; i++ {
				c.Assert(p[i].String(), qt.Equals, fmt.Sprintf(`test-topic,top=%d name="",value=0 1571243250000000000`, i+1))
			}
		},
	}, {
		about: "fields",
		config: exporter.TopicConfig{
			// By not declaring Type, Type = "field" is assumed
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
	}, {
		about: "constants",
		config: exporter.TopicConfig{
			Topic: "test-topic",
			Constants: map[string]interface{}{
				"a": 1,
				"b": "",
			},
		},
		data: map[string]interface{}{
			"b": "this will be ignored",
		},
		timestamps: []time.Time{
			time.Date(2019, 5, 1, 12, 0, 0, 0, time.UTC),
		},
		assertBatches: func(c *qt.C, points client.BatchPoints) {
			p := points.Points()
			c.Assert(p, qt.HasLen, 1)
			point := p[0]
			c.Assert(point.String(), qt.Equals, fmt.Sprintf(`test-topic a=1i,b="" 1556712000000000000`))
		},
	}, {
		about: "fields-with-tags",
		config: exporter.TopicConfig{
			Topic: "test-topic",
			Fields: map[string]string{
				"a": "number",
				"b": "string",
			},
			Tags: map[string]string{
				"tag1":      "t1",
				"something": "somevalue",
			},
		},
		data: map[string]interface{}{
			"a": 42,
			"b": "just a string",
		},
		timestamps: []time.Time{
			time.Date(2019, 5, 1, 12, 0, 0, 0, time.UTC),
		},
		assertBatches: func(c *qt.C, points client.BatchPoints) {
			p := points.Points()
			c.Assert(p, qt.HasLen, 1)
			point := p[0]
			c.Assert(point.String(), qt.Equals, fmt.Sprintf(`test-topic,something=somevalue,tag1=t1 a=42,b="just a string" 1556712000000000000`))
		},
	}, {
		about: "fields-with-tags-with-field-ref",
		config: exporter.TopicConfig{
			Topic: "test-topic",
			Fields: map[string]string{
				"a": "number",
			},
			Tags: map[string]string{
				"tag1": "t1",
				"tagA": "$b",
			},
		},
		data: map[string]interface{}{
			"a": 42,
			"b": "tagValue",
		},
		timestamps: []time.Time{
			time.Date(2019, 5, 1, 12, 0, 0, 0, time.UTC),
		},
		assertBatches: func(c *qt.C, points client.BatchPoints) {
			p := points.Points()
			c.Assert(p, qt.HasLen, 1)
			point := p[0]
			c.Assert(point.String(), qt.Equals, fmt.Sprintf(`test-topic,tag1=t1,tagA=tagValue a=42 1556712000000000000`))
		},
	}, {
		about: "timestamp accuracy",
		config: exporter.TopicConfig{
			Topic: "test-topic",
			Fields: map[string]string{
				"a": "number",
				"b": "string",
				"d": "number",
			},
			TimestampAccuracy: "d",
		},
		data: map[string]interface{}{
			"a": 42,
			"b": "just a string",
			"c": 5,
		},
		timestamps: []time.Time{
			time.Date(2019, 5, 1, 12, 3, 5, 7, time.UTC),
		},
		assertBatches: func(c *qt.C, points client.BatchPoints) {
			p := points.Points()
			c.Assert(p, qt.HasLen, 1)
			point := p[0]
			c.Assert(point.String(), qt.Equals, fmt.Sprintf(`test-topic a=42,b="just a string" 1556668800000000000`))
		},
	}, {
		about: "timestamp field",
		config: exporter.TopicConfig{
			Topic: "test-topic",
			Fields: map[string]string{
				"a": "number",
				"b": "string",
				"d": "number",
			},
			TimestampField:    "ts",
			TimestampAccuracy: "d",
		},
		data: map[string]interface{}{
			"a":  42,
			"b":  "just a string",
			"c":  5,
			"ts": "2019-01-02T15:04:05Z",
		},
		timestamps: []time.Time{
			time.Date(2019, 5, 1, 12, 3, 5, 7, time.UTC),
		},
		assertBatches: func(c *qt.C, points client.BatchPoints) {
			p := points.Points()
			c.Assert(p, qt.HasLen, 1)
			point := p[0]
			c.Assert(point.String(), qt.Equals, fmt.Sprintf(`test-topic a=42,b="just a string" 1546387200000000000`))
		},
	}, {
		about: "measurement name",
		config: exporter.TopicConfig{
			Topic:       "test-topic",
			Measurement: "something_completely_different",
			Fields: map[string]string{
				"a": "number",
			},
		},
		data: map[string]interface{}{
			"a": 42,
		},
		timestamps: []time.Time{
			time.Date(2019, 5, 1, 12, 0, 0, 0, time.UTC),
		},
		assertBatches: func(c *qt.C, points client.BatchPoints) {
			p := points.Points()
			c.Assert(p, qt.HasLen, 1)
			point := p[0]
			c.Assert(point.String(), qt.Equals, fmt.Sprintf(`something_completely_different a=42 1556712000000000000`))
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

func TestLogMessages(t *testing.T) {
	c := qt.New(t)

	c.AddCleanup(func() {
		log.SetOutput(os.Stderr)
	})

	topNExceeds := make(map[string]int)
	// 103 is from maxTopNEntries + 3
	for i := 0; i < 103; i++ {
		topNExceeds["entry"+strconv.Itoa(i)] = i
	}
	topNExceedsJSON, _ := json.Marshal(topNExceeds)

	tests := []struct {
		about       string
		config      exporter.TopicConfig
		message     string
		logContains string
	}{{
		about: "log missing entry key in message",
		config: exporter.TopicConfig{
			Topic: "test-topic",
			Fields: map[string]string{
				"foo": "number",
				"bar": "string",
			},
		},
		message:     `{"bar":"baz"}`,
		logContains: `entry key "foo" not found in topic "test-topic" message map[bar:baz]`,
	}, {
		about: "log unknown entry type in config",
		config: exporter.TopicConfig{
			Topic: "test-topic",
			Fields: map[string]string{
				"foo": "number",
				"bar": "mystery",
			},
		},
		message:     `{"foo":1,"bar":"baz"}`,
		logContains: `unknown entry type "mystery" for entry key "bar" topic "test-topic"`,
	}, {
		about: "log tag reference not found",
		config: exporter.TopicConfig{
			Topic: "test-topic",
			Fields: map[string]string{
				"a": "number",
			},
			Tags: map[string]string{
				"tag1": "t1",
				"tagA": "$b",
			},
		},
		message:     `{"a":42}`,
		logContains: `field not found: tag "tagA" referencing field "b" in topic "test-topic" message map[a:42]`,
	}, {
		about: "log tag reference of invalid type",
		config: exporter.TopicConfig{
			Topic: "test-topic",
			Fields: map[string]string{
				"a": "number",
			},
			Tags: map[string]string{
				"tag1": "t1",
				"tagA": "$a",
			},
		},
		message:     `{"a":42, "b":"msg"}`,
		logContains: `tag values have to be strings: key "a" in topic "test-topic" message map[a:42 b:msg]`,
	}, {
		about: "log message unmarshal error",
		config: exporter.TopicConfig{
			Topic: "test-topic",
			Fields: map[string]string{
				"foo": "number",
				"bar": "string",
			},
		},
		message:     `}{`,
		logContains: `failed to unmarshal a data point`,
	}, {
		about: "timestamp field missing",
		config: exporter.TopicConfig{
			Topic: "test-topic",
			Fields: map[string]string{
				"a": "number",
			},
			TimestampField: "ts",
		},
		message:     `{"a": 4}`,
		logContains: `timestamp field "ts" not present or incorrectly formatted`,
	}, {
		about: "timestamp field invalid type",
		config: exporter.TopicConfig{
			Topic: "test-topic",
			Fields: map[string]string{
				"a": "number",
			},
			TimestampField: "ts",
		},
		message:     `{"a": 4, "ts": 5}`,
		logContains: `timestamp field "ts" not present or incorrectly formatted`,
	}, {
		about: "timestamp not rfc3339",
		config: exporter.TopicConfig{
			Topic: "test-topic",
			Fields: map[string]string{
				"a": "number",
			},
			TimestampField: "ts",
		},
		message:     `{"a": 4, "ts": "a long long time ago"}`,
		logContains: `failed to parse timestamp field value "a long long time ago"`,
	}, {
		about: "log top-n message exceeds size",
		config: exporter.TopicConfig{
			Topic:  "test-topic",
			Type:   "top-n",
			Number: 5,
		},
		message:     string(topNExceedsJSON),
		logContains: `top-n entry length of 103 exceeds limit of 100 in topic "test-topic"`,
	}}
	for i, test := range tests {
		c.Logf("running test %d: %s", i, test.about)
		var buf bytes.Buffer
		log.SetOutput(&buf)
		influxClient := newTestInfluxClient()
		err := exporter.ProcessData(context.Background(), test.config, influxClient, [][]byte{[]byte(test.message)},
			[]time.Time{time.Date(2019, 5, 1, 12, 0, 0, 0, time.UTC)})
		c.Check(err, qt.IsNil)
		c.Check(buf.String(), qt.Contains, test.logContains)
	}
}

func TestConfigValidation(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		about  string
		config exporter.Config
		error  string
	}{{
		about: "valid config, no timestamp accuracy",
		config: exporter.Config{
			Topics: []exporter.TopicConfig{{
				Topic: "test-topic",
				Fields: map[string]string{
					"foo": "number",
					"bar": "string",
				},
			}},
		},
		error: "",
	}, {
		about: "valid config, with timestamp accuracy",
		config: exporter.Config{
			Topics: []exporter.TopicConfig{{
				Topic:             "test-topic",
				TimestampAccuracy: "d",
				Fields: map[string]string{
					"foo": "number",
					"bar": "string",
				},
			}},
		},
		error: "",
	}, {
		about: "valid config for top-n",
		config: exporter.Config{
			Topics: []exporter.TopicConfig{{
				Topic:  "test-topic",
				Type:   "top-n",
				Number: 50,
			}},
		},
		error: "",
	}, {
		about: "invalid config, missing fields and constants",
		config: exporter.Config{
			Topics: []exporter.TopicConfig{{
				Topic: "test-topic",
			}},
		},
		error: `a 'fields' translation requires 'fields' or 'constants' to be specified (topic: "test-topic")`,
	}, {
		about: "invalid config, missing number in top-n",
		config: exporter.Config{
			Topics: []exporter.TopicConfig{{
				Topic: "test-topic",
				Type:  "top-n",
			}},
		},
		error: `a 'top-n' translation requires 'number' to be between 1 and 100 (topic: "test-topic")`,
	}, {
		about: "invalid config, number below minimum in top-n",
		config: exporter.Config{
			Topics: []exporter.TopicConfig{{
				Topic:  "test-topic",
				Type:   "top-n",
				Number: 0,
			}},
		},
		error: `a 'top-n' translation requires 'number' to be between 1 and 100 (topic: "test-topic")`,
	}, {
		about: "invalid config, number beyond maximum in top-n",
		config: exporter.Config{
			Topics: []exporter.TopicConfig{{
				Topic:  "test-topic",
				Type:   "top-n",
				Number: 102,
			}},
		},
		error: `a 'top-n' translation requires 'number' to be between 1 and 100 (topic: "test-topic")`,
	}, {
		about: "invalid config, invalid timestamp accuracy",
		config: exporter.Config{
			Topics: []exporter.TopicConfig{{
				Topic:             "test-topic",
				TimestampAccuracy: "boo",
				Fields: map[string]string{
					"foo": "number",
					"bar": "string",
				},
			}},
		},
		error: `invalid timestamp accuracy value: "boo", topic: "test-topic"`,
	}}
	for i, test := range tests {
		c.Logf("running test %d: %s", i, test.about)
		err := test.config.Validate()
		if test.error == "" {
			c.Check(err, qt.IsNil)
		} else {
			c.Check(err.Error(), qt.Equals, test.error)
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
