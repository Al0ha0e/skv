package skv

import (
	"bytes"
	"encoding/binary"

	"github.com/Al0ha0e/skv/storage"
	"github.com/Al0ha0e/skv/transaction"
)

type DB struct {
	store storage.Storage
	lm    *transaction.LockManager
}

func Open(path string) (*DB, error) {
	store, err := storage.OpenBitcask(path)
	if err != nil {
		return nil, err
	}

	ret := &DB{
		store: store,
		lm:    transaction.MakeLockManager(),
	}
	return ret, nil
}

func (db *DB) Get(key []byte) (value []byte, err error) {
	db.store.Lock()
	defer db.store.Unlock()
	return db.store.Get(key)
}

func (db *DB) Put(key []byte, value []byte) (err error) {
	db.store.Lock()
	defer db.store.Unlock()
	return db.store.Put(key, value)
}

func (db *DB) Increase32(key []byte, inc int32) (err error) {
	db.store.Lock()
	defer db.store.Unlock()
	value, err := db.store.Get(key)
	if err != nil {
		return err
	}
	ivalue := int32(0)
	if value != nil {
		err = binary.Read(bytes.NewBuffer(value), binary.BigEndian, &ivalue)
		if err != nil {
			return err
		}
	}
	buf := &bytes.Buffer{}
	binary.Write(buf, binary.BigEndian, ivalue+inc)
	return db.store.Put(key, buf.Bytes())
}

func (db *DB) Delete(key []byte) (err error) {
	db.store.Lock()
	defer db.store.Unlock()
	return db.store.Delete(key)
}

func (db *DB) StartTransaction() transaction.Transaction {
	return db.lm.MakeTwoPLInstance(db.store)
}

func (db *DB) Close() {
	db.store.Close()
}
