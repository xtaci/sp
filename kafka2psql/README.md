# kafka2psql

```
NAME:
   kafka2psql - Store Kafka Topic To PostgreSQL Table

USAGE:
   kafka2psql [global options] command [command options] [arguments...]

VERSION:
   0.1

COMMANDS:
     help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --brokers value     kafka brokers address (default: "localhost:9092")
   --wal value         topic name for consuming commit log (default: "WAL")
   --table value       table name in WAL to archive (default: "user_updates")
   --pq value          psql url (default: "postgres://127.0.0.1:5432/pipeline?sslmode=disable")
   --pq-tblname value  psql table name, aware of timeformat in golang (default: "log_20060102")
   --help, -h          show help (default: false)
   --version, -v       print the version (default: false)
```
