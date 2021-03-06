package external_tests

import (
	"github.com/galtsev/dstor/conf"
	"github.com/galtsev/dstor/kafka"
	"github.com/galtsev/dstor/util"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"math/rand"
	"testing"
	"time"
)

func LoadConfig(t *testing.T) *conf.Config {
	var cfg conf.Config
	data, err := ioutil.ReadFile("test_config.yaml")
	assert.NoError(t, err)
	assert.NoError(t, yaml.Unmarshal(data, &cfg))
	return &cfg
}

func TestKafka_OffsetStorage(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	N := 60
	cfg := LoadConfig(t)
	st := kafka.NewOffsetStorage(cfg.Kafka)
	nodeId := util.NewUID()
	st.SetNodeId(nodeId)
	update := func(partition int32, offset *int64) {
		*offset += rand.Int63n(30)
		st.OnFlush(partition, *offset)
	}

	partitions := []int32{0, 1, 2, 3}
	offsets := make([]int64, len(partitions))
	for i := 0; i < N; i++ {
		pid := rand.Intn(len(partitions))
		update(partitions[pid], &offsets[pid])
	}
	for i, partition := range partitions {
		assert.Equal(t, offsets[i], st.GetOffset(partition))
	}
	st.Close()
	st = kafka.NewOffsetStorage(cfg.Kafka)
	st.SetNodeId(nodeId)
	for i, partition := range partitions {
		assert.Equal(t, offsets[i], st.GetOffset(partition))
	}

}
