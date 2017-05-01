package injector

import (
	"dan/pimco"
	"dan/pimco/conf"
	"dan/pimco/influx"
	"dan/pimco/kafka"
	"dan/pimco/ldb"
	"fmt"
)

type Injector struct {
	cfg       conf.Config
	generator pimco.Generator
	storage   pimco.Storage
	reporter  pimco.Reporter
}

func New(cfg conf.Config) *Injector {
	return &Injector{
		cfg: cfg,
	}
}

func (j *Injector) Generator() pimco.Generator {
	if j.generator == nil {
		j.generator = pimco.NewGenerator(j.cfg.Gen)
	}
	return j.generator
}

func (j *Injector) Storage() pimco.Storage {
	if j.storage == nil {
		srv := j.cfg.Server
		if srv.Storage == srv.Reporter && j.reporter != nil {
			j.storage = j.reporter.(pimco.Storage)
		} else {
			switch srv.Storage {
			case "kafka":
				j.storage = kafka.NewCluster(j.cfg)
			case "influx":
				j.reporter = influx.New(j.cfg.Influx)
			case "file":
				j.storage = pimco.NewFileStorage(j.cfg)
			default:
				panic(fmt.Errorf("Unknown storage in cfg.Server.Storage: %s", srv.Storage))
			}
		}
	}
	return j.storage
}

func (j *Injector) Reporter() pimco.Reporter {
	if j.reporter == nil {
		srv := j.cfg.Server
		if srv.Storage == srv.Reporter && j.storage != nil {
			j.reporter = j.storage.(pimco.Reporter)
		} else {
			switch srv.Reporter {
			case "remote":
				j.reporter = ldb.NewReporter(j.cfg)
			case "influx":
				j.reporter = influx.New(j.cfg.Influx)
			default:
				panic(fmt.Errorf("Unknown reporter in cfg.Server.Reporter: %s", srv.Reporter))
			}
		}
	}
	return j.reporter
}
