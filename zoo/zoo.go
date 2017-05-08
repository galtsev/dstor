package zoo

import (
	. "dan/pimco/base"
	"encoding/json"
	"github.com/samuel/go-zookeeper/zk"
	"log"
	"strings"
	"time"
)

type ReporterRec struct {
	Addr       string
	Partitions []int32
}

type Zoo struct {
	conn   *zk.Conn
	root   string
	nodeId string
}

func New(servers []string, nodeId string) *Zoo {
	conn, _, err := zk.Connect(servers, time.Duration(5)*time.Second)
	Check(err)
	return &Zoo{
		conn:   conn,
		root:   "/pimco",
		nodeId: nodeId,
	}
}

func (z *Zoo) Close() {
	z.conn.Close()
}

func (z *Zoo) ensurePath(root string) {
	parts := strings.Split(strings.Trim(root, "/"), "/")
	log.Println("parts", parts)
	path := ""
	acl := zk.WorldACL(zk.PermAll)
	for _, part := range parts {
		path = path + "/" + part
		if ok, _, _ := z.conn.Exists(path); !ok {
			log.Printf("creating missing zk path %s", path)
			z.conn.Create(path, []byte{}, 0, acl)
		}
	}
}

func (z *Zoo) Register(host string, partitions []int32) {
	base := z.root + "/reporters"
	z.ensurePath(base)
	acl := zk.WorldACL(zk.PermAll)
	path := base + "/" + z.nodeId
	rec := ReporterRec{
		Addr:       host,
		Partitions: partitions,
	}
	data, err := json.Marshal(rec)
	Check(err)
	_, err = z.conn.Create(path, data, zk.FlagEphemeral, acl)
	Check(err)
}
