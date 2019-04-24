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
	"math"
	"math/rand"
	"os"
	"os/signal"
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

type TopicConfig struct {
	Topic  string            `yaml:"topic"`
	Tags   map[string]string `yaml:"tags"`
	Fields map[string]string `yaml:"fields"`
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

	consumers := make([]*Consumer, 0)
	go func() {
		tries := 0
		nextTime := (time.Duration(math.Exp2(float64(tries))) * time.Millisecond) + time.Duration(rand.Intn(100))
		timer := time.NewTimer(nextTime)
		topics := config.Topics

		for len(topics) > 0 {
			var tmpTopics []TopicConfig

			for _, topicConfig := range topics {
				consumer, err := startConsumer(ctx, config.kafkaBrokers(), tlsConfig, httpClient, topicConfig)
				if err != nil {
					log.Printf("failed to start consumer with topic: %s: %v", topicConfig.Topic, err)
					tmpTopics = append(tmpTopics, topicConfig)
				} else {
					consumers = append(consumers, consumer)
				}
			}
			topics = tmpTopics

			tries++
			nextTime = (time.Duration(math.Exp2(float64(tries))) * time.Millisecond) + time.Duration(rand.Intn(100))
			if nextTime > maxBackoff {
				log.Printf("next timer %+v surpasses the max backoff time of %+v, setting to max backoff time\n", nextTime.String(), maxBackoff.String())
				nextTime = maxBackoff
			}

			timer = time.NewTimer(nextTime)
			var failingTopics []string
			for _, topic := range topics {
				failingTopics = append(failingTopics, topic.Topic)
			}
			log.Printf("scheduling next retry: %+v, tries: %d, topics failing: %+v\n", nextTime.String(), tries+1, failingTopics)

			select {
			case <-ctx.Done():
				log.Println("context canceled, exiting backoff")
				return
			case <-timer.C:
			}
		}
	}()

	defer func() {
		for _, consumer := range consumers {
			err := consumer.Close()
			if err != nil {
				log.Printf("failed to stop consumer for topic %q: %v", consumer.Topic, err)
				continue
			}
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	select {
	case <-c:
		log.Println("got interrupt")
		return
	}
}

func startConsumer(ctx context.Context, kafkaBrokers string, tlsConfig *TLSConfig, influxClient client.Client, config TopicConfig) (*Consumer, error) {
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
		Consume: func(ctx context.Context, data [][]byte, timestamps []time.Time) error {
			points := make([]*client.Point, len(data))
			for i, datum := range data {
				var entry map[string]interface{}
				err := json.Unmarshal(datum, &entry)
				if err != nil {
					log.Printf("failed to unmarshal a data point: %v", err)
					continue
				}
				log.Printf("looking for fields: %v", config.Fields)
				entryC := make(map[string]interface{})
				for key, entryType := range config.Fields {
					entryValue, ok := entry[key]
					if !ok {
						log.Printf("entry key not found: %v", key)
						continue
					}
					switch entryType {
					case "number":
						entryC[key] = entryValue.(float64)
					case "string":
						entryC[key] = entryValue.(string)
					case "hist":
						vals := entryValue.(map[string]interface{})
						for k, v := range vals {
							entryC[k] = v.(float64)
						}
					default:
						log.Printf("unknown entry type %v", entryType)
					}
				}
				log.Printf("sending %v", entryC)
				p, err := client.NewPoint(
					config.Topic,
					config.Tags,
					entryC,
					timestamps[i],
				)
				if err != nil {
					log.Printf("failed to create a new data point")
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
				bp.AddPoints(points[:max-1])
				points = points[max:]

				if err = influxClient.Write(bp); err != nil {
					log.Printf("failed to send a batch of points: %v", err)
					continue
				}
			}

			return nil
		},
	}

	consumer, err := NewConsumer(consumerConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return consumer, nil
}
