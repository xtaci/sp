package main

import (
	"encoding/json"
	"log"
	"os"
	"time"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/Jeffail/gabs"
	"github.com/Shopify/sarama"
	"github.com/urfave/cli"
)

const (
	timeFormat = "2006-01-02T15:04"
	bucketName = "snapshot"
	identifier = "__offset__"
)

type Offset struct {
	KafkaIdentifier string
	KafkaOffset     int64
}

func main() {
	myApp := cli.NewApp()
	myApp.Name = "kafka2mgo"
	myApp.Usage = "Store Kafka Topic To MongoDB Collection"
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
			Name:  "mongo",
			Value: "mongodb://localhost/log",
			Usage: "mongodb url",
		},
		cli.StringFlag{
			Name:  "collection",
			Value: "log",
			Usage: "mongodb collection",
		},
		cli.StringFlag{
			Name:  "primarykey,PK",
			Value: "",
			Usage: "do upsert if primary key is not nil, format: https://github.com/Jeffail/gabs",
		},
		cli.DurationFlag{
			Name:  "commit-interval",
			Value: 30 * time.Second,
			Usage: "longest commit interval to MongoDB",
		},
	}
	myApp.Action = processor
	myApp.Run(os.Args)
}

func processor(c *cli.Context) error {
	sess, err := mgo.Dial(c.String("mongo"))
	if err != nil {
		log.Fatalln(err)
	}
	defer sess.Close()
	coll := sess.DB("").C(c.String("collection"))

	primKey := c.String("primarykey")
	if primKey != "" {
		coll.EnsureIndexKey(primKey)
	}

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
	var tOffset Offset
	if err := coll.Find(bson.M{"kafkaidentifier": identifier}).One(&tOffset); err == nil {
		offset = tOffset.KafkaOffset + 1
	} else {
		log.Println(err)
	}

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

	var pending []*sarama.ConsumerMessage
	commitTicker := time.NewTicker(c.Duration("commit-interval"))

	log.Println("started")
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			pending = append(pending, msg)
		case <-commitTicker.C:
			commit(coll, pending, primKey)
			pending = nil
		}
	}
}

func commit(c *mgo.Collection, pending []*sarama.ConsumerMessage, primkey string) {
	var tOffset Offset
	tOffset.KafkaIdentifier = identifier
	for _, v := range pending {
		doc := make(map[string]interface{})
		if err := json.Unmarshal(v.Value, &doc); err == nil {
			if primkey != "" {
				if jsonParsed, err := gabs.ParseJSON(v.Value); err == nil {
					key := jsonParsed.Path(primkey).Data()
					if _, err := c.Upsert(bson.M{primkey: key}, doc); err != nil {
						log.Println(err)
					}
				} else {
					log.Println(err)
				}
			} else {
				if err := c.Insert(doc); err != nil {
					log.Println(err)
				}
			}
		}
		tOffset.KafkaOffset = v.Offset
	}
	if len(pending) > 0 {
		c.Upsert(bson.M{"kafkaidentifier": identifier}, tOffset)
		log.Println("offset:", tOffset.KafkaOffset)
	}
	log.Println("written:", len(pending))
}
