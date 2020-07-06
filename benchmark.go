package main

import (
	"context"
	"crypto/tls"
	"flag"
	"github.com/rcrowley/go-metrics"
	"math/rand"
	crand "crypto/rand"
	"os"
	"time"
	"log"
	"strings"
	"fmt"
	"io"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/Shopify/sarama"
)

var (
	client string
	mode   string

	brokers   string
	topic     string
	partition int

	msgSize     int
	numMessages int

	value []byte

	username string
	password string
	sasl     bool

	numConsumers int
	sync         bool
)

func newUUID() (string, error) {
	uuid := make([]byte, 16)
	n, err := io.ReadFull(crand.Reader, uuid)
	if n != len(uuid) || err != nil {
		return "", err
	}
	uuid[8] = uuid[8]&^0xc0 | 0x80
	uuid[6] = uuid[6]&^0xf0 | 0x40
	return fmt.Sprintf("%x-%x-%x-%x-%x", uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:]), nil
}

func consumeConfluentKafkaGo() {

	group, _ := newUUID()

	config := &kafka.ConfigMap{
		"bootstrap.servers":        brokers,
		"enable.auto.commit":       true,
		"group.id":                 group,
		"default.topic.config":     kafka.ConfigMap{"auto.offset.reset": "earliest"},
		"enable.auto.offset.store": true,
		"auto.commit.interval.ms":  10000,
		"statistics.interval.ms":   5000,
		"api.version.request":      true,
	}

	if sasl {
		config.SetKey("security.protocol", "SASL_SSL")
		config.SetKey("sasl.mechanisms", "PLAIN")
		config.SetKey("sasl.username", username)
		config.SetKey("sasl.password", password)
		config.SetKey("enable.ssl.certificate.verification", false)
	}

	var start time.Time
	var stats *kafka.Stats

	done := make(chan bool)
	msgCount := 0

	for i := 0; i < numConsumers; i++ {
		go func() {
			c, err := kafka.NewConsumer(config)

			if err != nil {
				log.Printf("could not set up kafka consumer: %s", err.Error())
				os.Exit(1)
			}

			c.Subscribe(topic, func(consumer *kafka.Consumer, e kafka.Event) error {
				start = time.Now()
				log.Printf("Rebalance callback: %v", e)
				return nil
			})

			for msgCount < numMessages {
				ev := c.Poll(-1)
				if ev == nil {
					continue
				}
				switch e := ev.(type) {
				case *kafka.Message:
					msgCount++
				case *kafka.Stats:
					stats = e
					//fmt.Printf("stats: %s\n", e)
				default:
					fmt.Printf("Ignored %v\n", e)
				}
			}
			done <- true
		}()
	}

	<-done
	elapsed := time.Since(start)

	log.Println(stats)

	log.Printf("[confluent-kafka-go consumer] # msgs: %d, msg/s: %f", msgCount, (float64(msgCount) / elapsed.Seconds()))
}

func consumeConfluentKafkaGoChannel() {

	group, _ := newUUID()

	config := &kafka.ConfigMap{
		"bootstrap.servers":               brokers,
		"enable.auto.commit":              true,
		"group.id":                        group,
		"auto.offset.reset":               "earliest",
		"enable.auto.offset.store":        true,
		"auto.commit.interval.ms":         10000,
		"statistics.interval.ms":          5000,
		"go.events.channel.enable":        true,
		"session.timeout.ms":              6000,
		"go.application.rebalance.enable": true,
	}

	if sasl {
		config.SetKey("security.protocol", "SASL_SSL")
		config.SetKey("sasl.mechanisms", "PLAIN")
		config.SetKey("sasl.username", username)
		config.SetKey("sasl.password", password)
		config.SetKey("enable.ssl.certificate.verification", false)
	}

	c, err := kafka.NewConsumer(config)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Consumer %v\n", c)

	err = c.Subscribe(topic, nil)

	msgCount := 0

	startTime := time.Now()

	for msgCount <= numMessages {
		select {
		case ev := <-c.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				c.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				c.Unassign()
			case *kafka.Message:
				msgCount++
			case kafka.PartitionEOF:
				fmt.Printf("%% Reached %v\n", e)
			case kafka.Error:
				// Errors should generally be considered as informational, the client will try to automatically recover
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
			}
		}
	}

	elapsed := time.Since(startTime)
	log.Printf("[confluent-kafka-go channel consumer] # msgs: %d, msg/s: %f", msgCount, (float64(msgCount) / elapsed.Seconds()))
	c.Close()
}
func produceConfluentKafkaGo() {

	// ~14073/s
	// local kafka: 321869/s
	log.Printf("connecting to %s", brokers)
	log.Printf("sasl %v", sasl)
	config := &kafka.ConfigMap{
		"bootstrap.servers":       brokers,
		"socket.keepalive.enable": true,
		"metadata.max.age.ms":     180000,
		"request.timeout.ms":      30000,
		"statistics.interval.ms":  5000,
		//"linger.ms":               100,
	}

	if sasl {
		config.SetKey("security.protocol", "SASL_SSL")
		config.SetKey("sasl.mechanisms", "PLAIN")
		config.SetKey("sasl.username", username)
		config.SetKey("sasl.password", password)
		config.SetKey("enable.ssl.certificate.verification", false)
	}

	var p, err = kafka.NewProducer(config)
	if err != nil {
		log.Printf("could not set up kafka producer: %s", err.Error())
		os.Exit(1)
	}

	msgCount := 0
	done := make(chan bool)
	var stats *kafka.Stats
	go func() {
		for e := range p.Events() {
			switch msg := e.(type) {
			case *kafka.Message:
				if msg.TopicPartition.Error != nil {
					log.Printf("delivery report error: %v", msg.TopicPartition.Error)
					continue
				}
				msgCount++
				if msgCount >= numMessages {
					done <- true
				}
			case kafka.Error:
				log.Printf("error: %v\n", msg)
			case *kafka.Stats:
				stats = msg

			default:
				log.Printf("unknown event: %v\n", msg)
			}
		}
	}()

	defer p.Close()

	var start = time.Now()
	for j := 0; j < numMessages; j++ {
		p.ProduceChannel() <- &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}, Value: value}
	}
	<-done
	elapsed := time.Since(start)

	log.Println(stats)
	log.Printf("[confluent-kafka-go producer] msg/s: %f", (float64(msgCount) / elapsed.Seconds()))
}

func produceConfluentKafkaGoSync() {

	log.Printf("connecting to %s", brokers)
	log.Printf("sasl %v", sasl)
	config := &kafka.ConfigMap{
		"bootstrap.servers":       brokers,
		"socket.keepalive.enable": true,
		"metadata.max.age.ms":     180000,
		"request.timeout.ms":      30000,
		"statistics.interval.ms":  5000,
		//"linger.ms":               100,
	}

	if sasl {
		config.SetKey("security.protocol", "SASL_SSL")
		config.SetKey("sasl.mechanisms", "PLAIN")
		config.SetKey("sasl.username", username)
		config.SetKey("sasl.password", password)
	}

	var p, err = kafka.NewProducer(config)
	if err != nil {
		log.Printf("could not set up kafka producer: %s", err.Error())
		os.Exit(1)
	}

	msgCount := 0
	var stats *kafka.Stats
	go func() {
		for e := range p.Events() {
			switch msg := e.(type) {
			case kafka.Error:
				log.Printf("error: %v\n", msg)
			case *kafka.Stats:
				stats = msg

			default:
				log.Printf("unknown event: %v\n", msg)
			}
		}
	}()

	defer p.Close()

	var start = time.Now()
	deliveryChan := make(chan kafka.Event)
	for j := 0; j < numMessages; j++ {
		err = p.Produce(&kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}, Value: value}, deliveryChan)
		if err != nil {
			log.Printf("Delivery failed")
			os.Exit(1)
		}

		e := <-deliveryChan
		m := e.(*kafka.Message)

		if m.TopicPartition.Error != nil {
			log.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
		} else {
			msgCount++
		}
	}
	elapsed := time.Since(start)

	log.Println(stats)
	log.Printf("[confluent-kafka-go producer sync] msg/s: %f", (float64(msgCount) / elapsed.Seconds()))
}

func consumeSarama() {

	groupId, _ := newUUID()

	config := sarama.NewConfig()
	config.Version = sarama.V1_0_0_0
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.MetricRegistry = metrics.DefaultRegistry

	if sasl {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = username
		config.Net.SASL.Password = password
		config.Net.SASL.Mechanism = "PLAIN"

		config.Net.TLS.Enable = true
		config.Net.TLS.Config = &tls.Config{
			InsecureSkipVerify: true,
			ClientAuth:         0,
		}
	}

	brokers := []string{brokers}

	// Create new consumer
	master, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		panic(err)
	}

	defer master.Close()

	// Create Consumer
	client, err := sarama.NewConsumerGroup(brokers, groupId, config)
	if err != nil {
		panic(err)
	}

	doneCh := make(chan bool)
	consumer := Consumer{
		doneCh:      doneCh,
		numMessages: numMessages,
	}

	var start = time.Now()
	go func() {
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := client.Consume(context.Background(), []string{topic}, &consumer); err != nil {
				log.Printf("Error from consumer: %v", err)
			}
		}
	}()

	<-doneCh
	elapsed := time.Since(start)

	log.Printf("[sarama consumer] # msgs: %d,  msg/s: %f", consumer.msgCount, (float64(consumer.msgCount) / elapsed.Seconds()))

	metrics.WriteOnce(metrics.DefaultRegistry, os.Stdout)
}

func produceSarama() {

	config := sarama.NewConfig()
	config.Version = sarama.V1_0_0_0
	config.Producer.Return.Successes = true
	config.MetricRegistry = metrics.DefaultRegistry

	if sasl {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = username
		config.Net.SASL.Password = password
		config.Net.SASL.Mechanism = "PLAIN"

		config.Net.TLS.Enable = true
		config.Net.TLS.Config = &tls.Config{
			InsecureSkipVerify: true,
			ClientAuth:         0,
		}
	}
	sarama.MaxRequestSize = 999000

	var p, err = sarama.NewAsyncProducer(strings.Split(brokers, ","), config)
	if err != nil {
		log.Printf("could not set up kafka producer: %s", err.Error())
		os.Exit(1)
	}

	msgCount := 0
	done := make(chan bool)
	go func() {
		for _ = range p.Successes() {
			msgCount++
			if msgCount >= numMessages {
				done <- true
			}
		}
	}()

	go func() {
		for err := range p.Errors() {
			log.Printf("failed to deliver message: %s", err.Error())
			os.Exit(1)
		}
	}()

	defer func() {
		err := p.Close()
		if err != nil {
			log.Printf("failed to close producer: %s", err.Error())
			os.Exit(1)
		}
	}()

	var start = time.Now()
	for j := 0; j < numMessages; j++ {
		p.Input() <- &sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(value)}
	}
	<-done
	elapsed := time.Since(start)

	metrics.WriteOnce(metrics.DefaultRegistry, os.Stdout)
	log.Printf("[sarama producer] # msgs %d, msg/s: %f", msgCount, (float64(msgCount) / elapsed.Seconds()))
}

func produceSaramaSync() {

	config := sarama.NewConfig()
	config.Version = sarama.V1_0_0_0
	config.Producer.Return.Successes = true
	config.MetricRegistry = metrics.DefaultRegistry

	if sasl {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = username
		config.Net.SASL.Password = password
		config.Net.SASL.Mechanism = "PLAIN"

		config.Net.TLS.Enable = true
		config.Net.TLS.Config = &tls.Config{
			InsecureSkipVerify: true,
			ClientAuth:         0,
		}
	}
	sarama.MaxRequestSize = 999000

	var p, err = sarama.NewSyncProducer(strings.Split(brokers, ","), config)
	if err != nil {
		log.Printf("could not set up kafka producer: %s", err.Error())
		os.Exit(1)
	}
	defer p.Close()

	msgCount := 0
	var start = time.Now()
	for j := 0; j < numMessages; j++ {
		_, _, err := p.SendMessage(&sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(value)})
		if err != nil {
			log.Printf("Error producing event: %v", err)
			os.Exit(1)
		}
		msgCount++
	}
	elapsed := time.Since(start)

	metrics.WriteOnce(metrics.DefaultRegistry, os.Stdout)
	log.Printf("[sarama producer] # msgs %d, msg/s: %f", msgCount, (float64(msgCount) / elapsed.Seconds()))
}

func main() {

	flag.StringVar(&brokers, "brokers", "localhost:9092", "broker addresses")
	flag.StringVar(&topic, "topic", "default", "topic")
	flag.IntVar(&partition, "partition", 0, "partition")
	flag.IntVar(&msgSize, "msgsize", 64, "message size")
	flag.IntVar(&numMessages, "numMessages", 100000, "number of messages")
	flag.StringVar(&client, "client", "sarama", "confluent-kafka-go / sarama / confluent-channel")
	flag.StringVar(&mode, "mode", "consumer", "producer / consumer")
	flag.BoolVar(&sync, "sync", false, "use synchronous style producer")
	flag.StringVar(&username, "username", "", "username")
	flag.StringVar(&password, "password", "", "password")
	flag.BoolVar(&sasl, "sasl", false, "use sasl")
	flag.IntVar(&numConsumers, "numConsumers", 1, "Number of consumers to create (only applies to confluent-kafka-go")
	flag.Parse()

	value = make([]byte, msgSize)
	rand.Read(value)

	switch client {

	case "confluent-kafka-go":
		if mode == "producer" {
			log.Print("Starting Confluent Kafka Producer")
			if sync {
				produceConfluentKafkaGoSync()
			} else {
				produceConfluentKafkaGo()
			}
		} else {
			consumeConfluentKafkaGo()
		}
		break

	case "confluent-channel":
		if mode == "producer" {
			log.Print("Starting Confluent Kafka Producer")
			if sync {
				produceConfluentKafkaGoSync()
			} else {
				produceConfluentKafkaGo()
			}
		} else {
			consumeConfluentKafkaGoChannel()
		}
		break

	case "sarama":
		if mode == "producer" {
			if sync {
				produceSaramaSync()
			} else {
				produceSarama()
			}
		} else {
			consumeSarama()
		}
		break

	default:
		log.Printf("unknown client: %s", client)
	}
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	doneCh      chan bool
	msgCount    int
	numMessages int
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	log.Printf("New claim its %v", claim)
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		session.MarkMessage(message, "")
		consumer.msgCount++
		if consumer.msgCount >= consumer.numMessages {
			consumer.doneCh <- true
			break
		}
	}

	return nil
}
