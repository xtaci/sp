package main

import (
	"fmt"
	"os"
	"time"

	"database/sql"

	"github.com/Jeffail/gabs"
	"github.com/lib/pq"

	"github.com/Shopify/sarama"

	log "github.com/Sirupsen/logrus"
	cli "gopkg.in/urfave/cli.v2"
)

const (
	reportInterval = 30 * time.Second
)

var consumerTblName = pq.QuoteIdentifier("kafka_consumer")

func main() {
	app := &cli.App{
		Name:    "kafka2psql",
		Usage:   `Store Kafka Topic To PostgreSQL Table`,
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
				Usage: "table name in WAL to archive",
			},
			&cli.StringFlag{
				Name:  "pq",
				Value: "postgres://127.0.0.1:5432/pipeline?sslmode=disable",
				Usage: "psql url",
			},
			&cli.StringFlag{
				Name:  "pq-tblname",
				Value: "log_20060102",
				Usage: "psql table name, aware of timeformat in golang",
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
	log.Println("pq:", c.String("pq"))
	log.Println("pq-tblname:", c.String("pq-tblname"))

	// unique consumer name to store in psql
	consumerId := fmt.Sprintf("%v-%v-%v-%v", c.String("wal"), c.String("table"), c.String("pq"), c.String("pq-tblname"))

	// connect to postgres
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

	// table creation
	db.Exec(fmt.Sprintf("CREATE TABLE %s (id TEXT PRIMARY KEY, value BIGINT)", consumerTblName))
	lastTblName := pq.QuoteIdentifier(time.Now().Format(c.String("pq-tblname")))
	db.Exec("CREATE TABLE " + lastTblName + "(id TEXT PRIMARY KEY, data JSONB)")

	// read offset
	offset := sarama.OffsetOldest
	err = db.QueryRow("SELECT value FROM kafka_consumer WHERE id = $1 LIMIT 1", consumerId).Scan(&offset)
	if err != nil {
		log.Println(err)
	}
	log.Println("consuming from offset:", offset)

	partitionConsumer, err := consumer.ConsumePartition(c.String("wal"), 0, offset)
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
	reportTicker := time.NewTicker(reportInterval)

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			// create new table if necessary
			tblName := pq.QuoteIdentifier(time.Now().Format(c.String("tblname")))
			if tblName != lastTblName {
				// CREATE TABLE
				db.Exec("CREATE TABLE " + tblName + "(id TEXT PRIMARY KEY, data JSONB)")
				lastTblName = tblName
			}

			commit(tblName, consumerId, db, msg)
			count++
		case <-reportTicker.C:
			log.Println("written:", count)
			count = 0
		}
	}
}

func commit(tblname, consumerId string, db *sql.DB, msg *sarama.ConsumerMessage) {
	// extract key
	var key string
	if jsonParsed, err := gabs.ParseJSON(msg.Value); err == nil {
		key = fmt.Sprint(jsonParsed.Path("key").Data())
	} else {
		log.Println(err)
		return
	}

	if r, err := db.Exec(fmt.Sprintf("INSERT INTO %s (id, data) VALUES ($1,$2) ON CONFLICT(id) DO UPDATE SET data = EXCLUDED.data",
		tblname), key, string(msg.Value)); err == nil {
		// write offset
		if r, err := db.Exec(fmt.Sprintf("INSERT INTO %s (id, value) VALUES ($1,$2) ON CONFLICT(id) DO UPDATE SET value=EXCLUDED.value",
			consumerTblName), consumerId, msg.Offset); err != nil {
			log.Println(r, err)
		}
	} else {
		log.Println(r, err)
	}
}
