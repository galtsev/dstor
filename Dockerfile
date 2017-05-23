#FROM golang:1.8
FROM golang:1.8-alpine

EXPOSE 8787
VOLUME /mnt/leveldb

COPY . /go/src/github.com/galtsev/dstor
COPY cmd/dstor.yaml /go

RUN go install github.com/galtsev/dstor/cmd