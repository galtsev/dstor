# Overview

This PoC implementation provide following functionality:

- `POST /save` to store samples
    expected body is json in form `{"time":<timestamp>, "tag":<string>, "values"[<float>,...]}`
- `GET /api?tag=<string>&start=<timestamp>&end<timestamp>` to retrieve report

, where `<timestamp>` is integer - nanosecond unixtime.

There is two modes of operation: standalone and cluster.

# Installation

Suppose that you have go1.8 installed and GOPATH configured:

    go get github.com/galtsev/dstor
    go install github.com/galtsev/dstor/dstor

This produce single executable `dstor` which can operate in different modes, depending on first command-line argument (subcommand).
For list of subcommands run `dstor -help`. For command-line options of specific subcommand run `dstor <subcommand> -help`.
Executable expect `dstor.yaml` file exists in current working directory, which contain runtime configuration. Current configuration
with all options can be retrieved with `dstor show-config`.

# Standalone server

Run it with:

    dstor standalone [args]

Standalone server have no other dependencies and good for evaluation. All accepted samples
stored in local embedded leveldb instance.

# Cluster mode

3rd party requirements are Kafka and Zookeeper.

Cluster consists of Proxy nodes and Storage nodes.

- *Proxy* node face client requests. It put accepted samples to kafka and proxy report requests to appropriate Storage node.
Proxy node is stateless and you can create as many proxy nodes as you need.
- *Storage* node continuously consume kafka topic, store consumed samples in local leveldb instance and respond to
report requests from Proxy nodes. Mapping between kafka partitions and storage nodes is manual (-partitions command-line parameter).
You can map same partition to several storage nodes - this is the way to provide redundancy. It is safe to restart storage node
or create additional storage nodes while cluster running. New/restarted storage node became available for report requests
as soon as it consume the most recent records from corresponding kafka partitions.

# Run cluster in Docker

    cd dstor
    docker build -t galtsev/dstor .
    docker-compose up -d zookeeper kafka node0 proxy

See provided `docker-compose.yml` for details.

# Test client

The same executable can be used to generate write load. Adjust `gen` section of `dstor/dstor.yaml` and run `dstor client`.