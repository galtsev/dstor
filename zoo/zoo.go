package zoo

import (
	. "dan/pimco/base"
	"dan/pimco/util"
	"encoding/json"
	"github.com/samuel/go-zookeeper/zk"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"
)

type ReporterRec struct {
	Addr       string
	Partitions []int32
}

type Zoo struct {
	sync.Mutex
	conn             *zk.Conn
	root             string
	reporterRegistry map[int32][]string
}

func New(servers []string) *Zoo {
	conn, _, err := zk.Connect(servers, time.Duration(5)*time.Second)
	Check(err)
	zoo := &Zoo{
		conn: conn,
		root: "/pimco",
	}
	go zoo.watchReporters()
	return zoo
}

func (z *Zoo) Close() {
	z.conn.Close()
}

func (z *Zoo) watchReporters() {
	for {
		reg := make(map[int32][]string)
		path := z.root + "/reporters"
		nodes, _, ch, err := z.conn.ChildrenW(path)
		Check(err)
		for _, nodeName := range nodes {
			data, _, err := z.conn.Get(path + "/" + nodeName)
			Check(err)
			var rec ReporterRec
			Check(json.Unmarshal(data, &rec))
			for _, partition := range rec.Partitions {
				reg[partition] = append(reg[partition], rec.Addr)
			}
		}
		z.Lock()
		z.reporterRegistry = reg
		z.Unlock()
		// wait for changes
		<-ch
	}
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
	rec := ReporterRec{
		Addr:       host,
		Partitions: partitions,
	}
	data, err := json.Marshal(rec)
	Check(err)
	path := base + "/" + util.NewUID()
	_, err = z.conn.Create(path, data, zk.FlagEphemeral, acl)
	Check(err)
}

func (z *Zoo) GetReporter(partition int32) string {
	z.Lock()
	defer z.Unlock()
	nodes, ok := z.reporterRegistry[partition]
	if !ok {
		return ""
	}
	return nodes[rand.Intn(len(nodes))]
}
