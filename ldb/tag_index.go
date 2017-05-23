package ldb

import (
	. "github.com/galtsev/dstor/base"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"strconv"
	"sync"
)

type TagIndex struct {
	db     *leveldb.DB
	lock   sync.RWMutex
	cache  map[string]int
	lastId int
}

func NewTagIndex(path string) *TagIndex {
	db, err := leveldb.OpenFile(path, nil)
	Check(err)
	t := TagIndex{
		db:    db,
		cache: make(map[string]int),
	}
	iter := db.NewIterator(&util.Range{}, nil)
	for iter.Next() {
		v, err := strconv.Atoi(string(iter.Value()))
		Check(err)
		t.cache[string(iter.Key())] = v
		if v > t.lastId {
			t.lastId = v
		}
	}
	return &t
}

func (t *TagIndex) Close() {
	t.db.Close()
}

func (t *TagIndex) add(tag string) int {
	t.lock.Lock()
	defer t.lock.Unlock()
	// re-check cache after write lock aquired
	idx, ok := t.cache[tag]
	if ok {
		return idx
	}
	t.lastId += 1
	v := strconv.Itoa(t.lastId)
	err := t.db.Put([]byte(tag), []byte(v), nil)
	Check(err)
	t.cache[tag] = t.lastId
	return t.lastId
}

func (t *TagIndex) GetOrCreate(tag string) int {
	idx, ok := t.Get(tag)
	if !ok {
		idx = t.add(tag)
	}
	return idx
}

func (t *TagIndex) Get(tag string) (idx int, ok bool) {
	t.lock.RLock()
	idx, ok = t.cache[tag]
	t.lock.RUnlock()
	return idx, ok
}
