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
	"strconv"
	"strings"
	"time"

	client "github.com/influxdata/influxdb1-client/v2"
	"github.com/juju/clock"
	"github.com/juju/errors"
	yaml "gopkg.in/yaml.v1"
)

const maxBackoff = time.Minute * 5

var (
	kafkaBrokers = os.Getenv("KAFKA_BROKERS")
	influxAPI    = os.Getenv("INFLUX_API")
	configFile   = os.Getenv("CONFIG")
)

type TopicConfig struct {
	Topic     string            `yaml:"topic"`
	Type      string            `yaml:"type,omitempty"`
	Tags      map[string]string `yaml:"tags,omitempty"`
	Fields    map[string]string `yaml:"fields,omitempty"`
	KeyFormat string            `yaml:"key-format,omitempty"`
}

type Config struct {
	KafkaBrokers string        `yaml:"kafka-brokers,omitempty"`
	KafkaTLS     *tlsConfig    `yaml:"kafka-tls,omitempty"`
	InfluxDB     string        `yaml:"influx-db,omitempty"`
	Topics       []TopicConfig `yaml:"topics"`
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

func (c *dataConsumer) process(ctx context.Context, data [][]byte, timestamps []time.Time) error {
	points := make([]*client.Point, len(data))
	for i, datum := range data {
		var entry map[string]interface{}
		err := json.Unmarshal(datum, &entry)
		if err != nil {
			log.Printf("failed to unmarshal a data point %q: %v", string(datum), err)
			continue
		}
		entryC := make(map[string]interface{})
		switch c.config.Type {
		case "histogram", "top-k":
			for key, value := range entry {
				k := key
				if c.config.KeyFormat != "" {
					v, err := strconv.Atoi(key)
					if err == nil {
						k = fmt.Sprintf(c.config.KeyFormat, v)
					} else {
						log.Printf("warning: failed to parse integer key %q: %v", key, err)
					}
				}
				entryC[k] = value.(float64)
			}
		default:
			for key, entryType := range c.config.Fields {
				entryValue, ok := entry[key]
				if !ok {
					log.Printf("entry key %q not found", key)
					continue
				}
				switch entryType {
				case "number":
					entryC[key] = entryValue.(float64)
				case "string":
					entryC[key] = entryValue.(string)
				default:
					log.Printf("unknown entry type %q", entryType)
				}
			}
		}
		if len(entryC) == 0 {
			log.Printf("empty data point for %#v", entryC)
			continue
		}
		p, err := client.NewPoint(
			c.config.Topic,
			c.config.Tags,
			entryC,
			timestamps[i],
		)
		if err != nil {
			log.Printf("failed to create data point for %#v: %v", entry, err)
			continue
		}
		points[i] = p
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
