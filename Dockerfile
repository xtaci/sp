FROM golang:alpine
MAINTAINER xtaci <daniel820313@gmail.com>
RUN go get github.com/xtaci/sp/kafka2bolt
RUN go get github.com/xtaci/sp/kafka2psql
RUN go get github.com/xtaci/sp/joiner
