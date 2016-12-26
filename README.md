# Stream Processors on Kafka in Golang
[![Build Status][1]][2]
[1]: https://travis-ci.org/xtaci/sp.svg?branch=master
[2]: https://travis-ci.org/xtaci/sp

A Swiss army knife for kafka + golang stream processing.
![swiss](swiss-army.jpg)

## Available Tools
1. kafka2bolt -- continuously archive kafka topic to boltdb
2. kafka2psql -- continuously insert messages from kafka to PostgreSQL
3. joiner -- continuously join stream to table


## Installations
```
go get -u github.com/xtaci/sp/kafka2bolt
go get -u github.com/xtaci/sp/kafka2psql
go get -u github.com/xtaci/sp/joiner
```

## Message Format
All tools above will input/output json message from Kafka, in format:
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

* host: where this message was generated
* instanceId : who generated this message
* key: a unique id (events) or primary key(table) of this message
* table: a sub category of this message, shares a common schema
* type: a higher category of this message
