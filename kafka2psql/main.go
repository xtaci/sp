package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"database/sql"

	"github.com/Jeffail/gabs"
	"github.com/lib/pq"

	"github.com/Shopify/sarama"
	"github.com/urfave/cli"
)

const (
	reportInterval = 30 * time.Second
)

var consumerTblName = pq.QuoteIdentifier("kafka_consumer")

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	myApp := cli.NewApp()
	myApp.Name = "kafka2psql"
	myApp.Usage = `Store Kafka Topic To PostgreSQL Table`
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
			Name:  "pq",
			Value: "postgres://127.0.0.1:5432/pipeline?sslmode=disable",
			Usage: "psql url",
		},
		cli.StringFlag{
			Name:  "tblname",
			Value: "log_20060102",
			Usage: "psql table name, aware of timeformat in golang",
		},
		cli.StringFlag{
			Name:  "consumer",
			Value: "log",
			Usage: "consumer name to differs offsets in psql table:" + consumerTblName,
		},
		cli.StringFlag{
			Name:  "primarykey,PK",
			Value: "",
			Usage: "primary key path in json, if empty, message key will treated as key, format: https://github.com/Jeffail/gabs",
		},
		cli.BoolFlag{
			Name:  "appendonly",
			Usage: "append message only, will omit --primarykey, and use offset as the primary key",
		},
	}
	myApp.Action = processor
	myApp.Run(os.Args)
}

func processor(c *cli.Context) error {
	db, err := sql.Open("postgres", c.String("pq"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	log.Println("brokers:", c.StringSlice("brokers"))
	log.Println("topic:", c.String("topic"))
	log.Println("pq:", c.String("pq"))
	log.Println("tblname:", c.String("tblname"))
	log.Println("consumer:", c.String("consumer"))
	log.Println("primarykey:", c.String("primarykey"))
	log.Println("appendonly:", c.String("appendonly"))

	consumer, err := sarama.NewConsumer(c.StringSlice("brokers"), nil)
	if err != nil {
		log.Fatalln(err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// table creation
	db.Exec(fmt.Sprintf("CREATE TABLE %s (id TEXT PRIMARY KEY, value BIGINT)", consumerTblName))
	lastTblName := pq.QuoteIdentifier(time.Now().Format(c.String("tblname")))
	db.Exec("CREATE TABLE " + lastTblName + "(id TEXT PRIMARY KEY, data JSON)")

	// read offset
	offset := sarama.OffsetOldest
	err = db.QueryRow("SELECT value FROM kafka_consumer WHERE id = $1 LIMIT 1", c.String("consumer")).Scan(&offset)
	if err != nil {
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

	log.Println("started")
	var count int64
	primKey := c.String("primarykey")
	appendOnly := c.Bool("appendonly")
	reportTicker := time.NewTicker(reportInterval)

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			// create new table if necessary
			tblName := pq.QuoteIdentifier(time.Now().Format(c.String("tblname")))
			if tblName != lastTblName {
				// CREATE TABLE
				db.Exec("CREATE TABLE " + tblName + "(id TEXT PRIMARY KEY, data JSON)")
				lastTblName = tblName
			}

			commit(tblName, primKey, appendOnly, db, msg, c)
			count++
		case <-reportTicker.C:
			log.Println("written:", count)
			count = 0
		}
	}
}

func commit(tblname, primkey string, appendonly bool, db *sql.DB, msg *sarama.ConsumerMessage, c *cli.Context) {
	// compute key
	var key string
	if appendonly {
		key = fmt.Sprint(msg.Offset)
	} else if primkey != "" {
		if jsonParsed, err := gabs.ParseJSON(msg.Value); err == nil {
			key = fmt.Sprint(jsonParsed.Path(primkey).Data())
		} else {
			log.Println(err)
			return
		}
	} else {
		key = string(msg.Key)
	}

	if r, err := db.Exec(fmt.Sprintf("INSERT INTO %s (id, data) VALUES ($1,$2) ON CONFLICT(id) DO UPDATE SET data = EXCLUDED.data",
		tblname), key, string(msg.Value)); err == nil {
		// write offset
		if r, err := db.Exec(fmt.Sprintf("INSERT INTO %s (id, value) VALUES ($1,$2) ON CONFLICT(id) DO UPDATE SET value=EXCLUDED.value",
			consumerTblName), c.String("consumer"), msg.Offset); err != nil {
			log.Println(r, err)
		}
	} else {
		log.Println(r, err)
	}
}
