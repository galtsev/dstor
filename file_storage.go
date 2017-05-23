package dstor

import (
	"bufio"
	. "github.com/galtsev/dstor/base"
	"github.com/galtsev/dstor/conf"
	"github.com/galtsev/dstor/model"
	"github.com/galtsev/dstor/serializer"
	"io"
	"os"
	"sync"
)

type FileStorage struct {
	f   io.Closer
	w   *bufio.Writer
	szr serializer.Serializer
	ch  chan model.Sample
	wg  sync.WaitGroup
}

func NewFileStorage(cfg conf.Config) *FileStorage {
	f, err := os.Create(cfg.FilePath)
	Check(err)
	fs := &FileStorage{
		szr: serializer.MsgPackSerializer{},
		w:   bufio.NewWriter(f),
		f:   f,
		ch:  make(chan model.Sample, 1000),
	}
	fs.wg.Add(1)
	go fs.saver()
	return fs
}

func (fs *FileStorage) AddSample(sample *model.Sample, offset int64) {
	fs.ch <- *sample
}

func (fs *FileStorage) saver() {
	for sample := range fs.ch {
		data := fs.szr.Marshal(&sample)
		_, err := fs.w.Write(data)
		Check(err)
	}
	fs.wg.Done()
}

func (fs *FileStorage) Close() {
	close(fs.ch)
	fs.wg.Wait()
	fs.w.Flush()
	fs.f.Close()
}
