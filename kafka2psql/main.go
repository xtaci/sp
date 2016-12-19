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
	identifier     = "__offset__"
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
			Name:  "primkey",
			Value: "",
			Usage: "primary key path in json, if empty, message offset will treated as key, format: https://github.com/Jeffail/gabs",
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

	consumer, err := sarama.NewConsumer(c.StringSlice("brokers"), nil)
	if err != nil {
		log.Fatalln(err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// kafka offset table creation
	db.Exec(fmt.Sprintf("CREATE TABLE %s (id TEXT PRIMARY KEY, value BIGINT)", consumerTblName))

	// read offset
	offset := sarama.OffsetOldest
	db.QueryRow("SELECT value FROM kafka_consumer LIMIT 1").Scan(&offset)
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

	commitTicker := time.NewTicker(reportInterval)

	log.Println("started")

	var count int64
	lastTblName := pq.QuoteIdentifier(time.Now().Format(c.String("tblname")))
	db.Exec("CREATE TABLE " + lastTblName + "(id TEXT PRIMARY KEY, data JSON)")
	primKey := c.String("primkey")

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

			commit(tblName, primKey, db, msg, c)
			count++
		case <-commitTicker.C:
			log.Println("written:", count)
			count = 0
		}
	}
}

func commit(tblName, primKey string, db *sql.DB, msg *sarama.ConsumerMessage, c *cli.Context) {
	// write message
	var offset int64
	key := fmt.Sprint(msg.Offset)
	if primKey != "" {
		if jsonParsed, err := gabs.ParseJSON(msg.Value); err == nil {
			key = fmt.Sprint(jsonParsed.Path(primKey).Data())
		} else {
			log.Println(err)
		}
	}

	if r, err := db.Exec(fmt.Sprintf("INSERT INTO %s (id, data) VALUES ($1,$2) ON CONFLICT(id) DO UPDATE SET data = EXCLUDED.data",
		tblName), key, string(msg.Value)); err != nil {
		log.Println(r, err)
	}
	offset = msg.Offset

	// write offset
	if r, err := db.Exec(fmt.Sprintf("INSERT INTO %s (id, value) VALUES ($1,$2) ON CONFLICT(id) DO UPDATE SET value=EXCLUDED.value",
		consumerTblName), identifier, offset); err != nil {
		log.Println(r, err)
	}
}
