# Stream Processors on Kafka in Golang
[![Build Status][1]][2]
[1]: https://travis-ci.org/xtaci/sp.svg?branch=master
[2]: https://travis-ci.org/xtaci/sp

## Available Tools
1. archiver -- continuously archive kafka topic to boltdb
2. joiner -- continuously join stream to table
3. kafka2mgo -- continuously insert messages from kafka to mongodb
4. kafka2psql -- continuously insert messages from kafka to PostgreSQL


## Installations
```
go get github.com/xtaci/sp/archiver
go get github.com/xtaci/sp/kafka2mgo
go get github.com/xtaci/sp/kafka2psql
go get github.com/xtaci/sp/joiner
```

## Message Format
All tools above are based on a json message format like:
```
{
  "created_at": "2016-12-23T10:23:59.947264032Z",
  "data": {},
  "host": "game-dev",
  "instanceId": "game1",
  "key": 1059730,
  "table": "event_updates",
  "type": "WAL"
}
```
