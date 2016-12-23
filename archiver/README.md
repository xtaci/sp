# archiver
```
$ ./archiver -h
NAME:
   archiver - Create Commit Log Snapshots from Kafka to BoltDB

USAGE:
   archiver [global options] command [command options] [arguments...]

VERSION:
   0.1

COMMANDS:
     help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --brokers value          kafka brokers address (default: "localhost:9092")
   --wal value              topic name for consuming commit log (default: "WAL")
   --table value            table name in WAL to archive (default: "event_updates")
   --base value             base snapshot path, created if file doesn't exists (default: "./snapshot.db")
   --snapshot value         snapshot path, aware of timeformat in golang (default: "./snapshot-20060102.db")
   --rotate value           backup rotate duration (default: 4h0m0s)
   --commit-interval value  longest commit interval to BoltDB (default: 30s)
   --help, -h               show help (default: false)
   --version, -v            print the version (default: false)
  
```
