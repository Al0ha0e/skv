package transaction

type Transaction interface {
	Get(key []byte) (value []byte, err error)
	GetForUpdate(key []byte) (value []byte, err error)
	Put(key []byte, value []byte) (err error)
	Increase32(key []byte, value int32) (err error)
	// PutBatch(kvs []storage.KV) (err error)
	Delete(key []byte) (err error)
	Commit() (err error)
	Abort() (err error)
}

type TxState = int

const (
	TxStateAborted TxState = iota
	TxStateCommitted
	TxStateRunning
)
