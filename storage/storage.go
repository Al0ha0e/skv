package storage

import "sync"

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
	Lock()
	Unlock()
}

type NaiveStorage struct {
	store map[string][]byte
	lock  sync.Mutex
}

func MakeNaiveStorage() *NaiveStorage {
	return &NaiveStorage{
		store: make(map[string][]byte),
	}
}

func (ns *NaiveStorage) Get(key []byte) (value []byte, err error) {
	return ns.store[string(key)], nil
}

func (ns *NaiveStorage) Put(key []byte, value []byte) (err error) {
	ns.store[string(key)] = value
	return nil
}

func (ns *NaiveStorage) PutBatch(kvs []KV) (err error) {
	for _, kv := range kvs {
		k := kv.Key
		v := kv.Value
		if v == nil {
			delete(ns.store, string(k))
		} else {
			ns.store[string(k)] = v
		}
	}
	return nil
}

func (ns *NaiveStorage) Delete(key []byte) (err error) {
	_, ok := ns.store[string(key)]
	if ok {
		delete(ns.store, string(key))
	}
	return nil
}

func (ns *NaiveStorage) Close() (err error) {
	return nil
}

func (ns *NaiveStorage) Lock() {
	ns.lock.Lock()
}

func (ns *NaiveStorage) Unlock() {
	ns.lock.Unlock()
}
