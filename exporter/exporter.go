package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"time"

	client "github.com/influxdata/influxdb1-client/v2"
	"github.com/juju/clock"
	"github.com/juju/errors"
	yaml "gopkg.in/yaml.v1"
)

const maxBackoff = time.Minute * 5
const maxTopNEntries = 100

var (
	kafkaBrokers = os.Getenv("KAFKA_BROKERS")
	influxAPI    = os.Getenv("INFLUX_API")
	configFile   = os.Getenv("CONFIG")
)

/* TopicConfig represents how message in a topic should be interpreted and
transformed.

- If `Type` is `fields`, empty or undeclared, you must define a `fields`
  mapping which will be used to fetch fields from the message.
- If `Type` is `top-k` or `histogram`, you may define a `key-format` string to
  format integer keys using the fmt.Sprintf function.
- If `Type` is `top-n`, you must defined a `number` integer which will be the
  `n` in `top-n`.
*/
type TopicConfig struct {
	Topic       string                 `yaml:"topic"`
	Measurement string                 `yaml:"measurement,omitempty"`
	Type        string                 `yaml:"type,omitempty"`
	Tags        map[string]string      `yaml:"tags,omitempty"`
	Fields      map[string]string      `yaml:"fields,omitempty"`
	Constants   map[string]interface{} `yaml:"constants,omitempty"`
	KeyFormat   string                 `yaml:"key-format,omitempty"`
	Number      int                    `yaml:"number,omitempty"`

	/* TimestampAccuracy specifies the accuracy to which timestamps will
	   be truncated before writing to influx.
	   If not set, timestamps will not be truncated.
	   Accepted values:
	     d, day
	     H, hour
	     m, minute
	     s, second
	*/
	TimestampAccuracy string `yaml:"timestamp-accuracy,omitempty"`
	TimestampField    string `yaml:"timestamp-field,omitempty"`
}

// GetTimestampAccuracy returns the accuracy the datapoint timestamp should
// be truncated to for this topic.
// Invalid values will result in no truncation. The TopicConfig should
// be validated before, using the Validate method.
func (c TopicConfig) GetTimestampAccuracy() time.Duration {
	switch c.TimestampAccuracy {
	case "d", "day":
		return time.Hour * 24
	case "H", "hour":
		return time.Hour
	case "m", "minute":
		return time.Minute
	case "s", "second":
		return time.Second
	default:
		return time.Duration(0)
	}
}

// Validate validates the TopicConfig.
func (c TopicConfig) Validate() error {
	// Validate Type and associated fields
	switch c.Type {
	case "fields", "":
		if len(c.Fields) == 0 && len(c.Constants) == 0 {
			return fmt.Errorf("a 'fields' translation requires 'fields' or 'constants' to be specified (topic: %q)", c.Topic)
		}
	case "top-n":
		if c.Number <= 0 || c.Number > maxTopNEntries {
			return fmt.Errorf("a 'top-n' translation requires 'number' to be between 1 and 100 (topic: %q)", c.Topic)
		}
	}
	// Validate the timestamp accuracy.
	switch c.TimestampAccuracy {
	case "d", "day":
	case "H", "hour":
	case "m", "minute":
	case "s", "second":
	case "":
	default:
		return fmt.Errorf("invalid timestamp accuracy value: %q, topic: %q", c.TimestampAccuracy, c.Topic)
	}
	return nil
}

func (c TopicConfig) measurementName() string {
	if c.Measurement == "" {
		return c.Topic
	}
	return c.Measurement
}

type Config struct {
	KafkaBrokers string        `yaml:"kafka-brokers,omitempty"`
	KafkaTLS     *tlsConfig    `yaml:"kafka-tls,omitempty"`
	InfluxDB     string        `yaml:"influx-db,omitempty"`
	Topics       []TopicConfig `yaml:"topics"`
}

// Validate validates the Config.
func (c Config) Validate() error {
	for _, t := range c.Topics {
		if err := t.Validate(); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

type tlsConfig struct {
	CACert string `yaml:"ca-cert"`
	Cert   string `yaml:"cert"`
	Key    string `yaml:"key"`
}

func (c *Config) kafkaBrokers() string {
	if c.KafkaBrokers != "" {
		return c.KafkaBrokers
	}
	return kafkaBrokers
}

func (c *Config) tls() (*TLSConfig, error) {
	if c.KafkaTLS == nil {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(c.KafkaTLS.Cert, c.KafkaTLS.Key)
	if err != nil {
		return nil, errors.Annotate(err, "failed to load client certificate and key")
	}
	caCertBytes, err := ioutil.ReadFile(c.KafkaTLS.CACert)
	if err != nil {
		return nil, errors.Annotate(err, "failed to read CA certificate")
	}
	pemData, _ := pem.Decode(caCertBytes)
	if pemData == nil {
		return nil, errors.New("failed to decode CA certificate")
	}
	caCert, err := x509.ParseCertificate(pemData.Bytes)
	if err != nil {
		return nil, errors.Annotate(err, "invalid CA certificate")
	}
	return &TLSConfig{
		Certificate:   cert,
		CACertificate: caCert,
	}, nil
}

func (c *Config) influxDB() (*client.HTTPConfig, error) {
	influxDBConnectionString := influxAPI
	if c.InfluxDB != "" {
		influxDBConnectionString = c.InfluxDB
	}

	cfg := &client.HTTPConfig{}
	// the connection string format is:
	// <username>:<password>@<ip>:<port>
	tokens := strings.Split(influxDBConnectionString, "@")
	switch len(tokens) {
	case 1:
		// username&password not provided
		cfg.Addr = tokens[0]
	case 2:
		userpass := strings.Split(tokens[0], ":")
		if len(userpass) != 2 {
			return nil, errors.Errorf("invalid username:password format: %v", tokens[0])
		}
		cfg.Username = userpass[0]
		cfg.Password = userpass[1]
		cfg.Addr = fmt.Sprintf("http://%v", tokens[1])
	}

	return cfg, nil
}

func main() {
	log.Println("starting exporter")

	data, err := ioutil.ReadFile(configFile)
	if err != nil {
		log.Fatalf("failed to read the config file: %v", err)
	}
	var config Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		log.Fatalf("failed to unmarshal the config file: %v", err)
	}
	err = config.Validate()
	if err != nil {
		log.Fatalf("invalid config: %v", err)
	}
	tlsConfig, err := config.tls()
	if err != nil {
		log.Fatalf("invalid TLS configuration: %v", err)
	}

	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()

	clientCfg, err := config.influxDB()
	if err != nil {
		log.Fatalf("invalid influxdb connection string: %v", err)
	}
	httpClient, err := client.NewHTTPClient(*clientCfg)
	if err != nil {
		log.Fatalf("failed to create http client: %v", err)
	}
	defer httpClient.Close()
	if _, err := httpClient.Query(client.Query{Command: "CREATE DATABASE kpi"}); err != nil {
		log.Fatalf("failed to create database: %v", err)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	consumers := make([]*Consumer, 0)
	for _, topicConfig := range config.Topics {
		consumer, err := startConsumer(ctx, config.kafkaBrokers(), tlsConfig, httpClient, topicConfig)
		if err != nil {
			log.Printf("failed to start consumer with topic: %s %v, exiting in 5s", topicConfig.Topic, err)

			timer := time.NewTimer(5 * time.Second)

			select {
			case <-timer.C:
				os.Exit(1)
			case <-c:
				os.Exit(1)
			}
		} else {
			consumers = append(consumers, consumer)
		}
	}

	defer func() {
		for _, consumer := range consumers {
			err := consumer.Close()
			if err != nil {
				log.Printf("failed to stop consumer for topic %q: %v", consumer.Topic, err)
				continue
			}
		}
	}()

	select {
	case <-c:
		log.Println("got interrupt")
		return
	}
}

func startConsumer(ctx context.Context, kafkaBrokers string, tlsConfig *TLSConfig, influxClient client.Client, config TopicConfig) (*Consumer, error) {
	c := &dataConsumer{
		config:       config,
		influxClient: influxClient,
	}
	consumerConfig := ConsumerConfig{
		Context:          ctx,
		Brokers:          strings.Split(kafkaBrokers, ","),
		TLSConfig:        tlsConfig,
		Topic:            config.Topic,
		GroupName:        "influx-consumer",
		Clock:            clock.WallClock,
		ConsumePeriod:    time.Minute,
		StartWaitTime:    30 * time.Second,
		MaximumCacheSize: 10000,
		Consume:          c.process,
	}

	consumer, err := NewConsumer(consumerConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return consumer, nil
}

type dataConsumer struct {
	config       TopicConfig
	influxClient client.Client
}

func addPoint(pointsList *[]*client.Point, tc TopicConfig, extraTags map[string]string, fields map[string]interface{}, ts time.Time) {
	tags := make(map[string]string)
	for key, value := range tc.Tags {
		tags[key] = value
	}
	for key, value := range extraTags {
		tags[key] = value
	}
	p, err := client.NewPoint(tc.measurementName(), tags, fields, ts)
	if err != nil {
		log.Printf("failed to create a new data point: %v", err)
		return
	}
	*pointsList = append(*pointsList, p)
}

type topEntry struct {
	Key   string
	Value float64
}

func getFieldsAndTags(entry map[string]interface{}, config TopicConfig) (map[string]string, map[string]interface{}) {
	topic := config.Topic

	tagsMap := make(map[string]string)
	for tag, value := range config.Tags {
		if len(value) > 1 && value[0] == '$' {
			key := value[1:]
			actualValue, ok := entry[key]
			if !ok {
				log.Printf("field not found: tag %q referencing field %q in topic %q message %+v", tag, key, topic, entry)
				continue
			}
			value, ok = actualValue.(string)
			if !ok {
				log.Printf("tag values have to be strings: key %q in topic %q message %+v", key, topic, entry)
				continue
			}
		}
		tagsMap[tag] = value
	}

	fieldsMap := make(map[string]interface{})
	for key, fieldType := range config.Fields {
		fieldValue, ok := entry[key]
		if !ok {
			log.Printf("entry key %q not found in topic %q message %+v", key, topic, entry)
			continue
		}
		switch fieldType {
		case "number":
			fieldsMap[key] = fieldValue.(float64)
		case "string":
			fieldsMap[key] = fieldValue.(string)
		default:
			log.Printf("unknown entry type %q for entry key %q topic %q", fieldType, key, topic)
		}
	}

	for key, constant := range config.Constants {
		fieldsMap[key] = constant
	}

	return tagsMap, fieldsMap
}

func (c *dataConsumer) process(ctx context.Context, data [][]byte, timestamps []time.Time) error {
	// Pre-allocate the array of points depending on the type of translation
	nPoints := len(data)
	if c.config.Type == "top-n" {
		nPoints = len(data) * c.config.Number
	}
	points := make([]*client.Point, 0, nPoints)

	for i, datum := range data {
		var entry map[string]interface{}
		err := json.Unmarshal(datum, &entry)
		if err != nil {
			log.Printf("failed to unmarshal a data point: %v, message: %s", err, string(datum))
			continue
		}
		ts := timestamps[i]
		// Optionally override timestamp with field in the data point.
		if c.config.TimestampField != "" {
			tsRaw, ok := entry[c.config.TimestampField].(string)
			if !ok {
				log.Printf("timestamp field %q not present or incorrectly formatted in data point %s", c.config.TimestampField, string(datum))
				continue
			}
			ts, err = time.Parse(time.RFC3339, tsRaw)
			if err != nil {
				log.Printf("failed to parse timestamp field value %q: %v", tsRaw, err)
				continue
			}
		}
		ts = ts.Truncate(c.config.GetTimestampAccuracy())
		entryC := make(map[string]interface{})
		switch c.config.Type {
		case "histogram", "top-k":
			for key, value := range entry {
				k := key
				if c.config.KeyFormat != "" {
					v, err := strconv.Atoi(key)
					if err == nil {
						k = fmt.Sprintf(c.config.KeyFormat, v)
					}
				}
				entryC[k] = value.(float64)
			}
			addPoint(&points, c.config, nil, entryC, ts)
		case "fields", "":
			tagsMap, fieldsMap := getFieldsAndTags(entry, c.config)
			addPoint(&points, c.config, tagsMap, fieldsMap, ts)
		case "top-n":
			if len(entry) > maxTopNEntries {
				log.Printf("top-n entry length of %d exceeds limit of %d in topic %q", len(entry), maxTopNEntries, c.config.Topic)
				continue
			}

			// Gather all entries
			topSize := len(entry)
			if topSize < c.config.Number {
				topSize = c.config.Number
			}
			n := 0
			topN := make([]topEntry, topSize)
			for key, value := range entry {
				topN[n] = topEntry{
					Key:   key,
					Value: value.(float64),
				}
				n += 1
			}

			// Sort to get actual top-n ranking
			sort.Slice(topN, func(a, b int) bool {
				return topN[a].Value > topN[b].Value
			})

			// Generate points only for the top-n subset required by the config
			for n = 0; n < c.config.Number; n++ {
				entryC["name"] = topN[n].Key
				entryC["value"] = topN[n].Value
				addPoint(&points, c.config, map[string]string{
					"top": strconv.Itoa(n + 1),
				}, entryC, ts)
			}
		}
	}

	for len(points) > 0 {
		bp, err := client.NewBatchPoints(
			client.BatchPointsConfig{
				Database:  "kpi",
				Precision: "ms",
			},
		)
		if err != nil {
			log.Printf("failed to create a batch of points: %v", err)
			continue
		}

		max := 5000
		if len(points) < max {
			max = len(points)
		}

		bp.AddPoints(points[:max])
		points = points[max:]

		if err = c.influxClient.Write(bp); err != nil {
			log.Printf("failed to send a batch of points: %v", err)
			continue
		}
	}

	return nil
}
