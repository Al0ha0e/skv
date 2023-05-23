package storage

type KV struct {
	Key   []byte
	Value []byte
}

type Storage interface {
	Get(key []byte) (value []byte, err error)
	Put(key []byte, value []byte) (err error)
	PutBatch(kvs []KV) (err error)
	Delete(key []byte) (err error)
	Close() (err error)
}

func DSB(sb Storage) {}
