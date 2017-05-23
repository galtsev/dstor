package injector

import (
	"fmt"
	"github.com/galtsev/dstor"
	"github.com/galtsev/dstor/conf"
	"github.com/galtsev/dstor/influx"
	"github.com/galtsev/dstor/kafka"
	"github.com/galtsev/dstor/ldb"
	"os"
)

func MakeBackend(name string, cfg conf.Config, offsetStorage dstor.OffsetStorage) (backend dstor.Backend) {
	switch name {
	case "leveldb":
		backend = ldb.NewCluster(cfg.Leveldb, offsetStorage)
	case "influx":
		backend = influx.New(cfg.Influx, nil)
	default:
		panic(fmt.Errorf("Unknown backend : %s", name))
	}
	return
}

func MakeStorage(name string, cfg conf.Config, offsetStorage dstor.OffsetStorage) (storage dstor.Storage) {
	switch name {
	case "kafka":
		storage = kafka.NewCluster(cfg)
	case "file":
		storage = dstor.NewFileStorage(cfg)
	default:
		storage = MakeBackend(name, cfg, offsetStorage)
	}
	return
}

func MakeReporter(name string, cfg conf.Config) (reporter dstor.Reporter) {
	return MakeBackend(name, cfg, nil)
}

func NodeId(storage dstor.Storage) string {
	if id, ok := storage.(dstor.NodeId); ok {
		return id.NodeId()
	} else {
		nodeId := os.Getenv("PIMCO_NODE_ID")
		if nodeId != "" {
			return nodeId
		}
	}
	panic(fmt.Errorf("NodeId not defined!"))
}
