// Copyright 2019 Canonical Ltd.  All rights reserved.

package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/juju/clock"
	"github.com/juju/errors"
	"github.com/juju/utils"
	"github.com/juju/zaputil"
	"github.com/juju/zaputil/zapctx"
	"go.uber.org/zap"
	tomb "gopkg.in/tomb.v2"
)

const clientID = "metamorphosis"

func init() {
	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
}

const (
	defaultMaximumCacheSize = 1000
	defaultStartWaitTime    = time.Second
)

var (
	newClient = func(addr []string, cfg *sarama.Config) (sarama.Client, error) {
		return sarama.NewClient(addr, cfg)
	}

	newConsumerGroupFromClient = func(groupID string, client sarama.Client) (sarama.ConsumerGroup, error) {
		return sarama.NewConsumerGroupFromClient(groupID, client)
	}

	sendMessage = func(client sarama.SyncProducer, msg *sarama.ProducerMessage) (int32, int64, error) {
		return client.SendMessage(msg)
	}

	newSyncProducerFromClient = func(client sarama.Client) (sarama.SyncProducer, error) {
		return sarama.NewSyncProducerFromClient(client)
	}

	// ErrorRetryLater is returned by the consumer, meaning that
	// it is not able to handle the request atm, but the request
	// may be safely retried later
	ErrorRetryLater = errors.New("not yet ready - retry later")
)

type consumerMessage struct {
	payload      []byte
	timestamp    time.Time
	consumedFunc func()
}

type consumerClaim struct {
	id      string
	session sarama.ConsumerGroupSession
	claim   sarama.ConsumerGroupClaim
}

// TLSConfig contains information needed by the client
// to connect to kafka via TLS.
type TLSConfig struct {
	Certificate        tls.Certificate
	CACertificate      *x509.Certificate
	InsecureSkipVerify bool
}

// tls returns a tls.Config.
func (c *TLSConfig) tls() *tls.Config {
	config := &tls.Config{
		Certificates: []tls.Certificate{
			c.Certificate,
		},
		InsecureSkipVerify: c.InsecureSkipVerify,
	}

	if c.CACertificate != nil {
		caCertPool := x509.NewCertPool()
		caCertPool.AddCert(c.CACertificate)
		config.RootCAs = caCertPool
	}
	config.BuildNameToCertificate()
	return config
}

// ConsumerConfig holds the configuration data for the consumer.
type ConsumerConfig struct {
	Context          context.Context
	Brokers          []string
	VersionOverride  *sarama.KafkaVersion
	TLSConfig        *TLSConfig
	Topic            string
	GroupName        string
	MaximumCacheSize int
	Clock            clock.Clock
	Consume          func(context.Context, [][]byte, []time.Time) error
	ConsumePeriod    time.Duration
	StartWaitTime    time.Duration

	ReadyFunction        func()
	NotificationFunction func(int)
}

func (c *ConsumerConfig) validate() error {
	if c.Context == nil {
		return errors.New("context not specified")
	}
	if len(c.Brokers) == 0 {
		return errors.New("brokers not specified")
	}
	if c.Topic == "" {
		return errors.New("topic not specified")
	}
	if c.GroupName == "" {
		return errors.New("consumer group name not specified")
	}
	if c.MaximumCacheSize == 0 {
		c.MaximumCacheSize = defaultMaximumCacheSize
	}
	if c.Clock == nil {
		c.Clock = clock.WallClock
	}
	if c.Consume == nil {
		return errors.New("consume method not specified")
	}
	if c.ConsumePeriod == 0 {
		c.ConsumePeriod = 5 * time.Minute
	}
	if c.StartWaitTime == 0 {
		c.StartWaitTime = defaultStartWaitTime
	}
	if c.VersionOverride == nil {
		c.VersionOverride = &sarama.V2_0_0_0
	}
	return nil
}

// Consumer is the kafka consumer that will periodically flushing to the given consume function
type Consumer struct {
	ConsumerConfig

	client sarama.Client
	group  sarama.ConsumerGroup
	t      tomb.Tomb

	muClaims sync.RWMutex
	claims   map[string]*consumerClaim

	muMessages sync.Mutex
	messages   []consumerMessage
}

// NewConsumer creates a new sarama consumer.
func NewConsumer(config ConsumerConfig) (*Consumer, error) {
	if err := config.validate(); err != nil {
		return nil, errors.Trace(err)
	}
	if config.GroupName == "" {
		return nil, errors.New("consumer group name not specified")
	}

	clientCfg := sarama.NewConfig()
	clientCfg.Producer.Return.Successes = true
	clientCfg.Version = sarama.V2_1_0_0
	clientCfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	clientCfg.ClientID = clientID
	if config.TLSConfig != nil {
		zapctx.Error(context.Background(), "setting TLS config")
		clientCfg.Net.TLS.Config = config.TLSConfig.tls()
		clientCfg.Net.TLS.Enable = true
	}

	client, err := newClient(config.Brokers, clientCfg)
	if err != nil {
		return nil, errors.Annotate(err, "unable to create new kafka client")
	}
	topics, err := client.Topics()
	if err != nil {
		return nil, errors.Annotate(err, "unable to retrieve topic(s) metadata")
	}
	found := false
	for _, topic := range topics {
		if config.Topic == topic {
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("unable to find topic %s in cluster", config.Topic)
	}

	group, err := newConsumerGroupFromClient(config.GroupName, client)
	if err != nil {
		return nil, errors.Trace(err)
	}

	consumer := &Consumer{
		ConsumerConfig: config,
		client:         client,
		group:          group,
		claims:         make(map[string]*consumerClaim),
	}

	go consumer.consumeLoop()

	worker := &periodicWorker{
		ctx:        config.Context,
		task:       consumer.consume,
		period:     consumer.ConsumePeriod,
		startDelay: time.Duration(rand.Int63n(int64(consumer.StartWaitTime))),
	}
	go worker.loop()

	return consumer, nil
}

// Close closes the reader stream preventing us from reading any more
// messages from it.
func (c *Consumer) Close() error {
	cleanups := []func() error{
		c.group.Close,
		c.client.Close,
	}
	for _, cleanup := range cleanups {
		if err := cleanup(); err != nil {
			log.Printf("failed to clean up consumer: %v", errors.Trace(err))
		}
	}
	return nil
}

func (c *Consumer) consumeLoop() error {
	for {
		err := c.group.Consume(c.Context, []string{c.Topic}, c)
		if err != nil {
			zapctx.Error(context.Background(), "consumer error", zaputil.Error(err))
		}
		select {
		case <-c.Context.Done():
			return nil
		case <-time.After(c.ConsumePeriod):
		}
	}
}

// Setup implements the sarama.ConsumerGroupHandler interface.
func (c *Consumer) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup implements the sarama.ConsumerGroupHandler interface.
func (c *Consumer) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim implements the sarma.ConsumerGroupHandler interface.
func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	uuid, err := utils.NewUUID()
	if err != nil {
		return errors.Trace(err)
	}
	newClaim := &consumerClaim{
		id:      uuid.String(),
		session: session,
		claim:   claim,
	}
	c.muClaims.Lock()
	c.claims[newClaim.id] = newClaim
	c.muClaims.Unlock()

	defer func() {
		c.muClaims.Lock()
		defer c.muClaims.Unlock()
		delete(c.claims, newClaim.id)
	}()

	if c.ReadyFunction != nil {
		c.ReadyFunction()
	}

	for msg := range claim.Messages() {
		if msg == nil {
			return nil
		}
		c.muMessages.Lock()
		c.messages = append(c.messages,
			consumerMessage{
				payload:   msg.Value,
				timestamp: msg.Timestamp,
				consumedFunc: func() {
					// when the payload is consumed, we mark the message
					// and offset.
					session.MarkMessage(msg, "")
					session.MarkOffset(c.Topic, claim.Partition(), msg.Offset+1, "")
				},
			},
		)
		numberOfMessages := len(c.messages)
		c.muMessages.Unlock()

		if numberOfMessages >= c.MaximumCacheSize {
			_, err := c.consume(c.t.Context(context.Background()))
			if err != nil {
				zapctx.Error(context.Background(), "failed to consume messages", zaputil.Error(err))
			}
		}

		select {
		case <-c.t.Dying():
			return nil
		default:
		}
	}
	return nil
}

func (c *Consumer) consume(ctx context.Context) (interface{}, error) {
	if err := ctx.Err(); err != nil {
		zapctx.Error(context.Background(), "exiting consumer", zaputil.Error(err))
		return nil, errors.Trace(err)
	}

	c.muMessages.Lock()
	messages := c.messages
	c.messages = []consumerMessage{}
	c.muMessages.Unlock()

	// if there's nothing to consume
	if len(messages) == 0 {
		return struct {
			ProcessedMessages int `json:"processed-messages"`
		}{
			ProcessedMessages: 0,
		}, nil
	}

	ackFunctions := []func(){}
	payloads := [][]byte{}
	timestamps := []time.Time{}
	for _, message := range messages {
		ackFunctions = append(ackFunctions, message.consumedFunc)
		payloads = append(payloads, message.payload)
		timestamps = append(timestamps, message.timestamp)
	}
	// consume payloads collected in the cache
	err := c.Consume(ctx, payloads, timestamps)
	if err != nil {
		err = c.writeFailed(ctx, payloads)
		if err != nil {
			zapctx.Error(ctx, "cannot write metrics to the failed topic", zap.Error(err))
		}
		return nil, errors.Trace(err)
	}

	// mark all messages as consumed
	for _, ack := range ackFunctions {
		if ack != nil {
			ack()
		}
	}

	// if set, trigger the notification function
	if err := ctx.Err(); err == nil && c.NotificationFunction != nil {
		c.NotificationFunction(len(payloads))
	}

	return struct {
		ProcessedPayloads int `json:"processed-payloads"`
	}{
		ProcessedPayloads: len(payloads),
	}, nil
}

// SetOffsetWithTimestamp will reset the current reader object with a timestamp equal or greater to ts
func (r *Consumer) SetOffsetWithTimestamp(ctx context.Context, ts time.Time) error {
	r.muClaims.Lock()
	defer r.muClaims.Unlock()

	if len(r.claims) == 0 {
		return errors.Trace(ErrorRetryLater)
	}

	timeUnix := int64(ts.UnixNano() / int64(time.Millisecond))
	for _, claim := range r.claims {
		offset, err := r.client.GetOffset(r.Topic, claim.claim.Partition(), timeUnix)
		if err != nil {
			zapctx.Error(ctx, "failed to retrieve offset at time", zaputil.Error(err))
			return errors.Trace(err)
		}
		zapctx.Debug(ctx, "setting consumer offset", zap.Int64("offset", offset), zap.String("topc", r.Topic))
		claim.session.ResetOffset(r.Topic, claim.claim.Partition(), offset, "")
	}
	return nil

}

// WriteFailed will write the payload to the "failed payloads" topic.
func (r *Consumer) writeFailed(ctx context.Context, payloads [][]byte) error {
	r.client.Config().Producer.Return.Successes = true
	writer, err := newSyncProducerFromClient(r.client)
	if err != nil {
		return errors.Trace(err)
	}

	for _, payload := range payloads {
		_, _, err = sendMessage(
			writer,
			&sarama.ProducerMessage{
				Topic: fmt.Sprintf("%s_failed", r.Topic),
				Value: sarama.ByteEncoder(payload),
			},
		)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

type periodicWorker struct {
	ctx        context.Context
	task       func(context.Context) (interface{}, error)
	period     time.Duration
	startDelay time.Duration
}

func (w *periodicWorker) loop() {
	<-time.After(w.startDelay)

	ticker := time.NewTicker(w.period)
	for {
		select {
		case <-ticker.C:
			w.task(w.ctx)
		case <-w.ctx.Done():
			return
		}
	}
}
