# joiner

```
$ ./joiner -h
NAME:
   joiner - Do Stream-Table Joining On stream.foreignkey = table.primarykey

USAGE:
   joiner [global options] command [command options] [arguments...]

VERSION:
   0.1

COMMANDS:
     help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --brokers value         kafka brokers address (default: "localhost:9092")
   --wal value             topic name for consuming commit log (default: "WAL")
   --table value           table name in WAL to JOIN (default: "user_updates")
   --stream value          the stream topic to do JOIN (default: "events")
   --foreignkey value      extract the json field as foreign key in stream messages, format: https://github.com/Jeffail/gabs (default: "a.b.c")
   --write-interval value  interval for cache writing (default: 30s)
   --output value          output stream for joined result (default: "joined")
   --help, -h              show help (default: false)
   --version, -v           print the version (default: false)
   
```
