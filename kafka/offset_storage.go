package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	. "github.com/galtsev/dstor/base"
	"github.com/galtsev/dstor/conf"
	"sync"
)

type OffsetStorage struct {
	lock   sync.Mutex
	nodeId string
	topic  string
	client sarama.Client
	om     sarama.OffsetManager
	poms   map[int32]sarama.PartitionOffsetManager
}

/* This wraps sarama.OffsetManager to store partition offsets
   Sarama.OffsetManager
   1) Hang if topic don't exists, so, we panic here if topic not exists
   2) Only update offset if new offset is greater than previous one
*/
func NewOffsetStorage(cfg conf.KafkaConfig) *OffsetStorage {
	konf := sarama.NewConfig()
	konf.Consumer.Offsets.Initial = sarama.OffsetOldest
	//konf.Consumer.Offsets.CommitInterval = time.Duration(10) * time.Millisecond
	client, err := sarama.NewClient(cfg.Hosts, konf)
	Check(err)
	topics, err := client.Topics()
	Check(err)
	if !strIn(topics, cfg.Topic) {
		panic(fmt.Errorf("Topic %s not registered in Kafka", cfg.Topic))
	}
	return &OffsetStorage{
		topic:  cfg.Topic,
		client: client,
		poms:   make(map[int32]sarama.PartitionOffsetManager),
	}
}

func (s *OffsetStorage) SetNodeId(nodeId string) {
	s.nodeId = nodeId
}

func (s *OffsetStorage) Close() {
	for _, pom := range s.poms {
		Check(pom.Close())
	}
	Check(s.om.Close())
	Check(s.client.Close())
}

func (s *OffsetStorage) GetPartitionOffsetManager(partition int32) sarama.PartitionOffsetManager {
	var err error
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.nodeId == "" {
		panic(fmt.Errorf("Node id not set"))
	}
	if s.om == nil {
		s.om, err = sarama.NewOffsetManagerFromClient(s.nodeId, s.client)
		Check(err)
	}
	pom, ok := s.poms[partition]
	if !ok {
		pom, err = s.om.ManagePartition(s.topic, partition)
		Check(err)
		s.poms[partition] = pom
	}
	return pom
}

func (s *OffsetStorage) GetOffset(partition int32) int64 {
	offset, _ := s.GetPartitionOffsetManager(partition).NextOffset()
	return offset
}

func (s *OffsetStorage) OnFlush(partition int32, offset int64) {
	pom := s.GetPartitionOffsetManager(partition)
	pom.MarkOffset(offset, "")
}

type FakeOffsetStorage struct {
	sync.Mutex
	offsets map[int32]int64
}

func NewFakeOffsetStorage() *FakeOffsetStorage {
	return &FakeOffsetStorage{
		offsets: make(map[int32]int64),
	}
}

func (s *FakeOffsetStorage) GetOffset(partition int32) int64 {
	s.Lock()
	defer s.Unlock()
	return s.offsets[partition]
}

func (s *FakeOffsetStorage) OnFlush(partition int32, offset int64) {
	s.Lock()
	defer s.Unlock()
	s.offsets[partition] = offset
}

func strIn(strings []string, key string) bool {
	for _, s := range strings {
		if s == key {
			return true
		}
	}
	return false
}
