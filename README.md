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
