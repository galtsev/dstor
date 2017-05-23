# Overview

This PoC implementation provide following functionality:

- `POST /save` to store samples
    expected body is json in form `{"time":<timestamp>, "tag":<string>, "values"[<float>,...]}`
- `GET /api?tag=<string>&start=<timestamp>&end<timestamp>` to retrieve report

, where `<timestamp>` is integer - nanosecond unixtime.

There is two modes of operation: standalone and cluster.

# Compillation

Suppose that this source code checked out to directory `pimco`

To compile executable locally (go 1.8 required):

    cd pimco/cmd
    go build

This produce single executable `cmd` which can operate in different modes, depending on first command-line argument (subcommand).
For list of subcommands run `./cmd -help`. For command-line options of specific subcommand run `./cmd <subcommand> -help`.
Executable expect `pimco.yaml` file exists in current working directory, which contain runtime configuration. Current configuration
with all options can be retrieved with `./cmd show-config`.

# Standalone server

Run it with:

    ./cmd standalone [args]

Standalone server have no other dependencies and good for evaluation. All accepted samples
stored in local embedded leveldb instance.

# Cluster mode

3rd party requirements are Kafka and Zookeeper.

Cluster consists of Proxy nodes and Storage nodes.
Proxy node face client requests. It put writen samples to kafka and proxy report requests to appropriate Storage node.
Storage node continuously consume kafka topic, store consumed samples in local leveldb instance and respond to
report requests from Proxy nodes.

# Run cluster in Docker

    cd pimco
    docker build -t galtsev/pimco .
    docker-compose up -d zookeeper kafka node0 proxy

See provided `docker-compose.yml` for details.

# Test client

The same executable can be used to generate write load. Adjust `gen` section of `cmd/pimco.yaml` and run `./cmd client`.