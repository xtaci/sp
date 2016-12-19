package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Jeffail/gabs"
	"github.com/Shopify/sarama"
	"github.com/boltdb/bolt"
	"github.com/urfave/cli"
)

const (
	offsetStream  = "__offset_stream__"
	offsetTable   = "__offset_table__"
	processorName = "stream-table join"
)

func main() {
	myApp := cli.NewApp()
	myApp.Name = processorName
	myApp.Usage = "Stream LEFT JOIN Table On Stream.fkid = Table.Id"
	myApp.Version = "0.1"
	myApp.Flags = []cli.Flag{
		cli.StringSliceFlag{
			Name:  "brokers, b",
			Value: &cli.StringSlice{"localhost:9092"},
			Usage: "kafka brokers address",
		},
		cli.StringFlag{
			Name:  "stream",
			Value: "events",
			Usage: "the stream to do JOIN",
		},
		cli.StringFlag{
			Name:  "foreignkey,fk",
			Value: "a.b.c",
			Usage: "use json field as foreign key in stream messages, format: https://github.com/Jeffail/gabs",
		},
		cli.StringFlag{
			Name:  "table",
			Value: "user_updates",
			Usage: "the stream as table to do JOIN",
		},
		cli.StringFlag{
			Name:  "file",
			Value: "join.db",
			Usage: "persisted table file",
		},
		cli.StringFlag{
			Name:  "workdir",
			Value: ".",
			Usage: "directory for boltdb",
		},
		cli.DurationFlag{
			Name:  "write-interval",
			Value: 30 * time.Second,
			Usage: "interval for table persistence",
		},
		cli.StringFlag{
			Name:  "output",
			Value: "merged",
			Usage: "output stream after JOIN",
		},
	}
	myApp.Action = processor
	myApp.Run(os.Args)
}

func processor(c *cli.Context) error {
	db, err := bolt.Open(c.String("workdir")+"/"+c.String("file"), 0666, nil)
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

	producer, err := sarama.NewAsyncProducer(c.StringSlice("brokers"), nil)
	if err != nil {
		log.Fatalln(err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// read database to memory
	memTable := make(map[string][]byte)
	streamOffset := sarama.OffsetOldest
	tableOffset := sarama.OffsetOldest

	db.View(func(tx *bolt.Tx) error {
		if b := tx.Bucket([]byte(processorName)); b != nil {
			if v := b.Get([]byte(offsetStream)); v != nil {
				streamOffset = int64(binary.LittleEndian.Uint64(v))
			}
			if v := b.Get([]byte(offsetTable)); v != nil {
				tableOffset = int64(binary.LittleEndian.Uint64(v))
			}

			c := b.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				memTable[string(k)] = v
			}
		}
		return nil
	})

	log.Printf("consuming from: stream:%v offset:%v  table:%v offset %v\n", c.String("stream"), streamOffset, c.String("table"), tableOffset)

	stream, err := consumer.ConsumePartition(c.String("stream"), 0, streamOffset)
	if err != nil {
		log.Fatalln(err)
	}

	table, err := consumer.ConsumePartition(c.String("table"), 0, tableOffset)
	if err != nil {
		log.Fatalln(err)
	}

	defer func() {
		if err := stream.Close(); err != nil {
			log.Fatalln(err)
		}

		if err := table.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	log.Println("started")
	ticker := time.NewTicker(c.Duration("write-interval"))
	for {
		select {
		case <-ticker.C:
			commit(db, memTable, streamOffset, tableOffset)
		case msg := <-table.Messages():
			tableOffset = msg.Offset
			memTable[string(msg.Key)] = msg.Value
		case msg := <-stream.Messages():
			streamOffset = msg.Offset

			if jsonParsed, err := gabs.ParseJSON(msg.Value); err == nil {
				key := fmt.Sprint(jsonParsed.Path(c.String("foreignkey")).Data())
				if v := memTable[key]; v != nil {
					merged := "{" +
						`"stream":` + string(msg.Value) + "," +
						`"table":` + string(v) +
						"}"
					producer.Input() <- &sarama.ProducerMessage{Topic: c.String("output"), Key: nil, Value: sarama.ByteEncoder([]byte(merged))}
					commitStreamOffset(db, streamOffset)
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
		if err := bucket.Put([]byte(offsetTable), buf1); err != nil {
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
	log.Println("committed:", len(memtable), "stream offset:", streamOffset, "table offset:", tableOffset)
}

func commitStreamOffset(db *bolt.DB, streamOffset int64) {
	if err := db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(processorName))
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(streamOffset))
		return bucket.Put([]byte(offsetStream), buf)
	}); err != nil {
		log.Fatalln(err)
	}
}
