FROM golang:alpine
MAINTAINER xtaci <daniel820313@gmail.com>
RUN go get github.com/xtaci/sp/archiver
RUN go get github.com/xtaci/sp/kafka2mgo
RUN go get github.com/xtaci/sp/kafka2psql
RUN go get github.com/xtaci/sp/joiner
