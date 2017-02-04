package main

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/boltdb/bolt"
	cli "gopkg.in/urfave/cli.v2"
)

const (
	bucketName = "snapshot"
	offsetKey  = "__offset__"
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

func main() {
	app := &cli.App{
		Name:    "kafka2bolt",
		Usage:   `Store Kafka Topic To BoltDB file`,
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
				Usage: "table name in WAL to archive",
			},
			&cli.StringFlag{
				Name:  "base",
				Value: "./snapshot.db",
				Usage: "base snapshot path, created if file doesn't exists",
			},
			&cli.StringFlag{
				Name:  "snapshot",
				Value: "./snapshot-20060102.db",
				Usage: "snapshot path, aware of timeformat in golang",
			},
			&cli.DurationFlag{
				Name:  "rotate",
				Value: 4 * time.Hour,
				Usage: "backup rotate duration",
			},
			&cli.DurationFlag{
				Name:  "commit-interval",
				Value: 30 * time.Second,
				Usage: "longest commit interval to BoltDB",
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
	base := c.String("base")
	snapshot := c.String("snapshot")
	rotate := c.Duration("rotate")
	commit_interval := c.Duration("commit-interval")

	log.Println("brokers:", brokers)
	log.Println("table-topic:", table_topic)
	log.Println("table:", table)
	log.Println("base:", base)
	log.Println("snapshot:", snapshot)
	log.Println("rotate:", rotate)
	log.Println("commit-interval:", commit_interval)

	db, err := bolt.Open(base, 0666, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	consumer, err := sarama.NewConsumer(brokers, nil)
	if err != nil {
		log.Fatalln(err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// read offset
	offset := sarama.OffsetOldest
	db.View(func(tx *bolt.Tx) error {
		if b := tx.Bucket([]byte(bucketName)); b != nil {
			if v := b.Get([]byte(offsetKey)); v != nil {
				offset = int64(binary.LittleEndian.Uint64(v))
			}
			log.Printf("%+v\n", b.Stats())
		}
		return nil
	})
	log.Printf("consuming from topic:%v offset:%v", table_topic, offset)

	partitionConsumer, err := consumer.ConsumePartition(table_topic, 0, offset)
	if err != nil {
		log.Fatalln(err)
	}

	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	pending := make(map[string][]byte)
	rotateTicker := time.NewTicker(rotate)
	commitTicker := time.NewTicker(commit_interval)

	log.Println("started")
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			wal := &WAL{}
			if err := json.Unmarshal(msg.Value, wal); err == nil {
				if wal.Table == table { // table filter
					pending[wal.Key] = msg.Value
					offset = msg.Offset
				}
			} else {
				log.Println(err)
			}
		case <-commitTicker.C:
			commit(db, pending, offset)
			pending = make(map[string][]byte)
		case <-rotateTicker.C:
			if err := db.View(func(tx *bolt.Tx) error {
				newfile := time.Now().Format(snapshot)
				log.Println("new boltdb file:", newfile)
				return tx.CopyFile(newfile, 0666)
			}); err != nil {
				log.Fatalln(err)
			}
		}
	}
}

func commit(db *bolt.DB, pending map[string][]byte, offset int64) {
	if err := db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return err
		}

		// write messages
		for key, value := range pending {
			if err = bucket.Put([]byte(key), value); err != nil {
				log.Println(err)
			}
		}

		// write offset
		offset_encode := make([]byte, 8)
		binary.LittleEndian.PutUint64(offset_encode, uint64(offset))
		if err = bucket.Put([]byte(offsetKey), offset_encode); err != nil {
			return err
		}
		log.Println("written:", len(pending), "offset:", offset)
		return nil
	}); err != nil {
		log.Fatalln(err)
	}
}
