package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/Jeffail/gabs"
	"github.com/Shopify/sarama"
	"github.com/boltdb/bolt"

	log "github.com/Sirupsen/logrus"
	cli "gopkg.in/urfave/cli.v2"
)

const (
	offsetStream  = "__offset_stream__"
	offsetWAL     = "__offset_wal__"
	processorName = "joiner"
	outputTable   = "joiner"
)

type WAL struct {
	Type       string          `json:"type"`
	InstanceId string          `json:"instanceId"`
	Table      string          `json:"table"`
	Host       string          `json:"host"`
	Key        string          `json:"key"`
	CreatedAt  time.Time       `json:"created_at"`
	Data       json.RawMessage `json:"data"`
}

type STJoin struct {
	Stream *json.RawMessage `json:"stream"`
	Table  *json.RawMessage `json:"table"`
}

func main() {
	app := &cli.App{
		Name:    processorName,
		Usage:   "Stream-Table joining on stream-topic.stream-key = table-topic.key",
		Version: "0.1",
		Flags: []cli.Flag{
			&cli.StringSliceFlag{
				Name:  "brokers, b",
				Value: cli.NewStringSlice("localhost:9092"),
				Usage: "kafka brokers address",
			},
			&cli.StringFlag{
				Name:  "table-topic",
				Value: "WAL",
				Usage: "topic name that contains the table",
			},
			&cli.StringFlag{
				Name:  "table",
				Value: "user_updates",
				Usage: "table name in WAL to JOIN",
			},
			&cli.StringFlag{
				Name:  "stream-topic",
				Value: "events",
				Usage: "the stream topic to do JOIN",
			},
			&cli.StringFlag{
				Name:  "stream-key",
				Value: "",
				Usage: "extract the json field as foreign key in stream messages, format: https://github.com/Jeffail/gabs",
			},
			&cli.StringFlag{
				Name:  "output-topic",
				Value: "",
				Usage: "default output topic name: joiner-{table-topic}-{table}-{stream}",
			},
			&cli.DurationFlag{
				Name:  "write-interval",
				Value: 30 * time.Second,
				Usage: "interval for cache writing",
			},
		},
		Action: processor,
	}
	app.Run(os.Args)
}

func processor(c *cli.Context) error {
	brokers := c.StringSlice("brokers")
	table_topic := c.String("table-topic")
	table := c.String("table")
	stream_topic := c.String("stream-topic")
	stream_key := c.String("stream-key")
	output_topic := c.String("output-topic")
	if output_topic == "" {
		output_topic = fmt.Sprintf("joiner-%v-%v-%v", table_topic, table, stream_topic)
	}
	write_interval := c.Duration("write-interval")

	log.Println("brokers:", brokers)
	log.Println("table-topic:", table_topic)
	log.Println("table:", table)
	log.Println("stream-topic:", stream_topic)
	log.Println("stream-key:", stream_key)
	log.Println("output-topic:", output_topic)
	log.Println("write-interval:", write_interval)

	cachefile := fmt.Sprintf(".joiner-%v-%v-%v.cache", table_topic, table, stream_topic)
	instanceId := fmt.Sprintf("%v-%v", processorName, os.Getpid())
	log.Println("cache file:", cachefile)
	log.Println("instanceId:", instanceId)

	if stream_key == "" {
		log.Fatalln("stream_key is not set")
	}

	db, err := bolt.Open(cachefile, 0666, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(processorName))
		return err
	}); err != nil {
		log.Fatalln(err)
	}

	consumer, err := sarama.NewConsumer(brokers, nil)
	if err != nil {
		log.Fatalln(err)
	}

	config := sarama.NewConfig()
	config.Producer.Return.Successes = false
	config.Producer.Return.Errors = false
	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		log.Fatalln(err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln(err)
		}
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// read database to memory
	memTable := make(map[string][]byte)
	streamOffset := sarama.OffsetNewest
	tableOffset := sarama.OffsetOldest

	db.View(func(tx *bolt.Tx) error {
		if b := tx.Bucket([]byte(processorName)); b != nil {
			if v := b.Get([]byte(offsetStream)); v != nil {
				streamOffset = int64(binary.LittleEndian.Uint64(v))
			}
			if v := b.Get([]byte(offsetWAL)); v != nil {
				tableOffset = int64(binary.LittleEndian.Uint64(v))
			}

			c := b.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				data := make([]byte, len(v))
				copy(data, v)
				memTable[string(k)] = data
			}
		}
		return nil
	})

	log.Printf("consuming from stream offset:%v table offset:%v", streamOffset, tableOffset)

	streamConsumer, err := consumer.ConsumePartition(stream_topic, 0, streamOffset)
	if err != nil {
		log.Fatalln(err)
	}

	tableConsumer, err := consumer.ConsumePartition(table_topic, 0, tableOffset)
	if err != nil {
		log.Fatalln(err)
	}

	defer func() {
		if err := streamConsumer.Close(); err != nil {
			log.Fatalln(err)
		}

		if err := tableConsumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	log.Println("started")
	ticker := time.NewTicker(write_interval)
	numJoined := 0

	// parameters
	host, _ := os.Hostname()
	for {
		select {
		case <-ticker.C:
			commit(db, memTable, streamOffset, tableOffset)
			log.Println("committed:", len(memTable), "stream offset:", streamOffset, "table offset:", tableOffset, "joined:", numJoined)
			numJoined = 0
		case msg := <-tableConsumer.Messages():
			tableOffset = msg.Offset
			wal := &WAL{}
			if err := json.Unmarshal(msg.Value, wal); err == nil {
				if wal.Table == table { // table filter
					memTable[wal.Key] = msg.Value
				}
			}
		case msg := <-streamConsumer.Messages():
			streamOffset = msg.Offset
			if jsonParsed, err := gabs.ParseJSON(msg.Value); err == nil {
				key := fmt.Sprint(jsonParsed.Path(stream_key).Data())
				t := memTable[key]
				wal := &WAL{}
				wal.Type = "AUGMENT"
				wal.InstanceId = instanceId
				wal.Table = outputTable
				wal.Host = host
				data, _ := json.Marshal(STJoin{Stream: (*json.RawMessage)(&msg.Value), Table: (*json.RawMessage)(&t)})
				wal.Data = data
				wal.Key = fmt.Sprint(msg.Offset) // offset is unique as primary key
				wal.CreatedAt = time.Now()
				if bts, err := json.Marshal(wal); err == nil {
					producer.Input() <- &sarama.ProducerMessage{Topic: output_topic, Value: sarama.ByteEncoder([]byte(bts))}
					numJoined++
				} else {
					log.Println(err)
				}
			}
		}
	}
}

func commit(db *bolt.DB, memtable map[string][]byte, streamOffset, tableOffset int64) {
	if err := db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(processorName))
		for k, v := range memtable {
			if err := bucket.Put([]byte(k), v); err != nil {
				return err
			}
		}

		buf1 := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf1, uint64(tableOffset))
		if err := bucket.Put([]byte(offsetWAL), buf1); err != nil {
			return err
		}

		buf2 := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf2, uint64(streamOffset))
		if err := bucket.Put([]byte(offsetStream), buf2); err != nil {
			return err
		}

		return nil
	}); err != nil {
		log.Fatalln(err)
	}
}
