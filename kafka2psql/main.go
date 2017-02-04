package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"database/sql"

	postgres "github.com/lib/pq"

	"github.com/Shopify/sarama"

	log "github.com/Sirupsen/logrus"
	cli "gopkg.in/urfave/cli.v2"
)

var consumerTblName = postgres.QuoteIdentifier("kafka_consumer")

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
				Name:  "pq",
				Value: "postgres://127.0.0.1:5432/pipeline?sslmode=disable",
				Usage: "psql url",
			},
			&cli.StringFlag{
				Name:  "pq-tblname",
				Value: "log20060102",
				Usage: "psql table name, aware of timeformat in golang",
			},
			&cli.DurationFlag{
				Name:  "commit-interval",
				Value: time.Second,
				Usage: "interval for committing pending data to psql",
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
	pq := c.String("pq")
	pq_tblname := c.String("pq-tblname")
	commit_interval := c.Duration("commit-interval")

	log.Println("brokers:", brokers)
	log.Println("table-topic:", table_topic)
	log.Println("table:", table)
	log.Println("pq:", pq)
	log.Println("pq-tblname:", pq_tblname)
	log.Println("commit-interval:", commit_interval)

	// unique consumer name to store in psql
	consumerId := fmt.Sprintf("%v-%v-%v", table_topic, table, pq_tblname)
	log.Println("consumerId:", consumerId)

	// connect to postgres
	db, err := sql.Open("postgres", pq)
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

	// table creation
	db.Exec(fmt.Sprintf("CREATE TABLE %s (id TEXT PRIMARY KEY, value BIGINT)", consumerTblName))
	lastTblName := postgres.QuoteIdentifier(time.Now().Format(pq_tblname))
	db.Exec("CREATE TABLE " + lastTblName + "(id TEXT PRIMARY KEY, data JSONB)")

	// read offset
	offset := sarama.OffsetOldest
	err = db.QueryRow("SELECT value FROM kafka_consumer WHERE id = $1 LIMIT 1", consumerId).Scan(&offset)
	if err != nil {
		log.Println(err)
	}
	log.Println("consuming from offset:", offset)

	tableConsumer, err := consumer.ConsumePartition(table_topic, 0, offset)
	if err != nil {
		log.Fatalln(err)
	}

	defer func() {
		if err := tableConsumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	log.Println("started")
	commitTicker := time.NewTicker(commit_interval)
	pending := make(map[string][]byte)

	for {
		select {
		case msg := <-tableConsumer.Messages():
			wal := &WAL{}
			if err := json.Unmarshal(msg.Value, wal); err == nil {
				if wal.Table == table { // table filter
					// create new table if necessary
					tblName := postgres.QuoteIdentifier(time.Now().Format(pq_tblname))
					if tblName != lastTblName {
						commit(lastTblName, consumerId, db, pending, offset)
						pending = make(map[string][]byte)
						// CREATE TABLE
						db.Exec("CREATE TABLE " + tblName + "(id TEXT PRIMARY KEY, data JSONB)")
						lastTblName = tblName
					}

					// pending
					pending[wal.Key] = msg.Value
					offset = msg.Offset
				}
			} else {
				log.Println(err)
			}
		case <-commitTicker.C:
			commit(lastTblName, consumerId, db, pending, offset)
			pending = make(map[string][]byte)
		}
	}
}

func commit(tblname, consumerId string, db *sql.DB, pending map[string][]byte, offset int64) {
	if len(pending) == 0 {
		return
	}

	for key, value := range pending {
		if r, err := db.Exec(fmt.Sprintf("INSERT INTO %s (id, data) VALUES ($1,$2) ON CONFLICT(id) DO UPDATE SET data = EXCLUDED.data",
			tblname), key, string(value)); err == nil {
		} else {
			log.Println(r, err)
		}
	}

	// write offset
	if r, err := db.Exec(fmt.Sprintf("INSERT INTO %s (id, value) VALUES ($1,$2) ON CONFLICT(id) DO UPDATE SET value=EXCLUDED.value",
		consumerTblName), consumerId, offset); err != nil {
		log.Println(r, err)
	}
	log.Println("written:", len(pending), "offset:", offset)
}
