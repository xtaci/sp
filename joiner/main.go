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
	Type       string      `json:"type"`
	InstanceId string      `json:"instanceId"`
	Table      string      `json:"table"`
	Host       string      `json:"host"`
	Key        string      `json:"key"`
	CreatedAt  time.Time   `json:"created_at"`
	Data       interface{} `json:"data"`
}

type STJoin struct {
	Stream *json.RawMessage `json:"stream"`
	Table  *json.RawMessage `json:"table"`
}

func main() {
	app := &cli.App{
		Name:    processorName,
		Usage:   "Stream-Table Joining On stream.foreignkey = table.primarykey",
		Version: "0.1",
		Flags: []cli.Flag{
			&cli.StringSliceFlag{
				Name:  "brokers, b",
				Value: cli.NewStringSlice("localhost:9092"),
				Usage: "kafka brokers address",
			},
			&cli.StringFlag{
				Name:  "wal",
				Value: "WAL",
				Usage: "topic name for consuming commit log",
			},
			&cli.StringFlag{
				Name:  "table",
				Value: "user_updates",
				Usage: "table name in WAL to JOIN",
			},
			&cli.StringFlag{
				Name:  "stream",
				Value: "events",
				Usage: "the stream topic to do JOIN",
			},
			&cli.StringFlag{
				Name:  "output",
				Value: "",
				Usage: "default output topic name: joiner-{wal}-{table}-{stream}",
			},
			&cli.StringFlag{
				Name:  "foreignkey,FK",
				Value: "",
				Usage: "extract the json field as foreign key in stream messages, format: https://github.com/Jeffail/gabs",
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
	log.Println("brokers:", c.StringSlice("brokers"))
	log.Println("wal:", c.String("wal"))
	log.Println("table:", c.String("table"))
	log.Println("stream:", c.String("stream"))
	log.Println("foreignkey:", c.String("foreignkey"))
	log.Println("write-interval:", c.Duration("write-interval"))

	outputTopic := c.String("output")
	if outputTopic == "" {
		outputTopic = fmt.Sprintf("joiner-%v-%v-%v", c.String("wal"), c.String("table"), c.String("stream"))
	}
	cachefile := fmt.Sprintf(".joiner-%v-%v-%v.cache", c.String("wal"), c.String("table"), c.String("stream"))
	instanceId := fmt.Sprintf("%v-%v", processorName, os.Getpid())
	log.Println("output:", outputTopic)
	log.Println("output table:", outputTable)
	log.Println("cache file:", cachefile)
	log.Println("instanceId:", instanceId)

	if c.String("foreignkey") == "" {
		log.Fatalln("foreignkey is not set")
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

	consumer, err := sarama.NewConsumer(c.StringSlice("brokers"), nil)
	if err != nil {
		log.Fatalln(err)
	}

	config := sarama.NewConfig()
	config.Producer.Return.Successes = false
	config.Producer.Return.Errors = false
	producer, err := sarama.NewAsyncProducer(c.StringSlice("brokers"), config)
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
	walOffset := sarama.OffsetOldest

	db.View(func(tx *bolt.Tx) error {
		if b := tx.Bucket([]byte(processorName)); b != nil {
			if v := b.Get([]byte(offsetStream)); v != nil {
				streamOffset = int64(binary.LittleEndian.Uint64(v))
			}
			if v := b.Get([]byte(offsetWAL)); v != nil {
				walOffset = int64(binary.LittleEndian.Uint64(v))
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

	log.Printf("consuming from: stream:%v offset:%v  wal:%v offset:%v", c.String("stream"), streamOffset, c.String("wal"), walOffset)

	stream, err := consumer.ConsumePartition(c.String("stream"), 0, streamOffset)
	if err != nil {
		log.Fatalln(err)
	}

	wal, err := consumer.ConsumePartition(c.String("wal"), 0, walOffset)
	if err != nil {
		log.Fatalln(err)
	}

	defer func() {
		if err := stream.Close(); err != nil {
			log.Fatalln(err)
		}

		if err := wal.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	log.Println("started")
	ticker := time.NewTicker(c.Duration("write-interval"))
	numJoined := 0

	// parameters
	host, _ := os.Hostname()
	for {
		select {
		case <-ticker.C:
			commit(db, memTable, streamOffset, walOffset)
			log.Println("committed:", len(memTable), "stream offset:", streamOffset, "wal offset:", walOffset, "joined:", numJoined)
			numJoined = 0
		case msg := <-wal.Messages():
			walOffset = msg.Offset
			if jsonParsed, err := gabs.ParseJSON(msg.Value); err == nil {
				if table := fmt.Sprint(jsonParsed.Path("table").Data()); table == c.String("table") {
					key := fmt.Sprint(jsonParsed.Path("key").Data())
					memTable[key] = msg.Value
				}
			}
		case msg := <-stream.Messages():
			streamOffset = msg.Offset
			if jsonParsed, err := gabs.ParseJSON(msg.Value); err == nil {
				key := fmt.Sprint(jsonParsed.Path(c.String("foreignkey")).Data())
				t := memTable[key]
				wal := &WAL{}
				wal.Type = "AUGMENT"
				wal.InstanceId = instanceId
				wal.Table = outputTable
				wal.Host = host
				wal.Data = STJoin{Stream: (*json.RawMessage)(&msg.Value), Table: (*json.RawMessage)(&t)}
				wal.Key = fmt.Sprint(msg.Offset) // offset is unique as primary key
				wal.CreatedAt = time.Now()
				if bts, err := json.Marshal(wal); err == nil {
					producer.Input() <- &sarama.ProducerMessage{Topic: outputTopic, Value: sarama.ByteEncoder([]byte(bts))}
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
