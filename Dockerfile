#FROM golang:1.8
FROM golang:1.8-alpine

COPY . /go/src/dan/pimco
COPY cmd/pimco.yaml /go

RUN go install dan/pimco/cmd