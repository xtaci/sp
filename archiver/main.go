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
	timeFormat = "2006-01-02T15:04"
	bucketName = "snapshot"
	offsetKey  = "__offset__"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	myApp := cli.NewApp()
	myApp.Name = "archiver"
	myApp.Usage = "Create Commit Log Snapshots from Kafka to BoltDB"
	myApp.Version = "0.1"
	myApp.Flags = []cli.Flag{
		cli.StringSliceFlag{
			Name:  "brokers, b",
			Value: &cli.StringSlice{"localhost:9092"},
			Usage: "kafka brokers address",
		},
		cli.StringFlag{
			Name:  "topic, t",
			Value: "commitlog",
			Usage: "topic name for consuming commit log",
		},
		cli.StringFlag{
			Name:  "file",
			Value: "snapshot.db",
			Usage: "snapshot file name",
		},
		cli.StringFlag{
			Name:  "primkey",
			Value: "",
			Usage: "use json field as primary key instead of message key, format: https://github.com/Jeffail/gabs",
		},
		cli.StringFlag{
			Name:  "workdir",
			Value: ".",
			Usage: "directory for boltdb",
		},
		cli.DurationFlag{
			Name:  "rotate",
			Value: 4 * time.Hour,
			Usage: "backup rotate duration",
		},
		cli.DurationFlag{
			Name:  "commit-interval",
			Value: 30 * time.Second,
			Usage: "longest commit interval to BoltDB",
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

	consumer, err := sarama.NewConsumer(c.StringSlice("brokers"), nil)
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
	log.Println("consuming from offset:", offset)

	partitionConsumer, err := consumer.ConsumePartition(c.String("topic"), 0, offset)
	if err != nil {
		log.Fatalln(err)
	}

	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	pending := make(map[string][]byte)
	rotateTicker := time.NewTicker(c.Duration("rotate"))
	commitTicker := time.NewTicker(c.Duration("commit-interval"))

	log.Println("started")
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			pending[string(msg.Key)] = msg.Value
			offset = msg.Offset
		case <-commitTicker.C:
			commit(c.String("primkey"), db, pending, offset)
			pending = make(map[string][]byte)
		case <-rotateTicker.C:
			if err := db.View(func(tx *bolt.Tx) error {
				return tx.CopyFile(c.String("workdir")+"/"+c.String("file")+"-"+time.Now().Format(timeFormat), 0600)
			}); err != nil {
				log.Fatalln(err)
			}
		}
	}
}

func commit(primkey string, db *bolt.DB, pending map[string][]byte, offset int64) {
	if err := db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return err
		}

		// write messages
		for key, value := range pending {
			if primkey != "" { // json field as primary key
				if jsonParsed, err := gabs.ParseJSON(value); err == nil {
					key := fmt.Sprint(jsonParsed.Path(primkey).Data())
					if err = bucket.Put([]byte(key), value); err != nil {
						log.Println(err)
					}
				} else {
					log.Println(err)
				}
			} else { // message key as primary key
				if err = bucket.Put([]byte(key), value); err != nil {
					log.Println(err)
				}
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
