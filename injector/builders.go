package injector

import (
	"dan/pimco"
	"dan/pimco/conf"
	"dan/pimco/influx"
	"dan/pimco/kafka"
	"dan/pimco/ldb"
	"fmt"
)

func MakeBackend(name string, cfg conf.Config, offsetStorage pimco.OffsetStorage) (backend pimco.Backend) {
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

func MakeStorage(name string, cfg conf.Config, offsetStorage pimco.OffsetStorage) (storage pimco.Storage) {
	switch name {
	case "kafka":
		storage = kafka.NewCluster(cfg)
	case "file":
		storage = pimco.NewFileStorage(cfg)
	default:
		storage = MakeBackend(name, cfg, offsetStorage)
	}
	return
}

func MakeReporter(name string, cfg conf.Config) (reporter pimco.Reporter) {
	switch name {
	case "remote":
		reporter = ldb.NewReporter(cfg)
	default:
		reporter = MakeBackend(name, cfg, nil)
	}
	return
}
