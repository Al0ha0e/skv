package transaction

import (
	"errors"
	"math"
	"sync"

	"github.com/Al0ha0e/skv/storage"
)

type VersionNode struct {
	Prev  *VersionNode
	RTS   uint32
	WTS   uint32
	Value []byte
}

func MakeVersionNode(prev *VersionNode, rts uint32, wts uint32, value []byte) *VersionNode {
	return &VersionNode{
		Prev:  prev,
		RTS:   rts,
		WTS:   wts,
		Value: value,
	}
}

type MVCCLockManager struct {
	Locks    map[string]int
	Versions map[string]*VersionNode
	Store    storage.Storage
	CurrTS   uint32
	latch    sync.Mutex
}

func MakeMVCCLockManager(store storage.Storage) *MVCCLockManager {
	return &MVCCLockManager{
		Locks:    make(map[string]int),
		Versions: make(map[string]*VersionNode),
		Store:    store,
		CurrTS:   0,
	}
}

func (lm *MVCCLockManager) getOriVersion(key []byte, skey string) (*VersionNode, error) {
	lm.Store.Lock()
	ori, err := lm.Store.Get(key)
	lm.Store.Unlock()
	if err != nil {
		return nil, err
	}
	if ori != nil {
		ret := MakeVersionNode(nil, 0, 0, ori)
		lm.Versions[skey] = ret
		return ret, nil
	}
	return nil, nil
}

func (lm *MVCCLockManager) Lock(key []byte, skey string, ts uint32) bool {
	lm.latch.Lock()
	defer lm.latch.Unlock()

	_, has := lm.Locks[skey]
	if has {
		return false
	}

	var node *VersionNode
	node, has = lm.Versions[skey]
	if !has {
		var err error
		node, err = lm.getOriVersion(key, skey)
		if err != nil {
			return false
		}
	}

	if node.RTS >= ts || node.WTS >= ts {
		return false
	}

	lm.Locks[skey] = 1
	return true
}

func (lm *MVCCLockManager) Unlock(key string) {
	lm.latch.Lock()
	delete(lm.Locks, key)
	lm.latch.Unlock()
}

func (lm *MVCCLockManager) UpdateVersion(skey string, value []byte, ts uint32) {
	lm.latch.Lock()
	prev := lm.Versions[skey]
	lm.Versions[skey] = MakeVersionNode(prev, ts, ts, value)
	lm.latch.Unlock()
}

func (lm *MVCCLockManager) Get(key []byte, skey string, ts uint32) ([]byte, error) {
	lm.latch.Lock()
	defer lm.latch.Unlock()

	node, has := lm.Versions[skey]
	if !has {
		var err error
		node, err = lm.getOriVersion(key, skey)
		if err != nil {
			return nil, err
		}
	}

	for ; node != nil; node = node.Prev {
		if node.WTS < ts {
			break
		}
	}
	node.RTS = uint32(math.Max(float64(node.RTS), float64(ts)))
	return node.Value, nil
}

type MVCCInstance struct {
	LM    *MVCCLockManager
	Store storage.Storage
	View  map[string][]byte
	Locks map[string]int
	State TxState
	TS    uint32
}

func (lm *MVCCLockManager) MakeMVCCInstance(store storage.Storage) *MVCCInstance {
	lm.latch.Lock()
	defer lm.latch.Unlock()

	lm.CurrTS++

	return &MVCCInstance{
		LM:    lm,
		Store: store,
		View:  make(map[string][]byte),
		Locks: make(map[string]int),
		State: TxStateRunning,
		TS:    lm.CurrTS,
	}
}

func (mvcc *MVCCInstance) unlockAllLocks() {
	for key := range mvcc.Locks {
		mvcc.LM.Unlock(key)
	}
}

func (mvcc *MVCCInstance) abort() {
	mvcc.unlockAllLocks()
	mvcc.State = TxStateAborted
}

func (mvcc *MVCCInstance) Get(key []byte) (value []byte, err error) {
	if mvcc.State != TxStateRunning {
		return nil, errors.New("tx not running")
	}

	skey := string(key)
	value, ok := mvcc.View[skey]
	if ok {
		return value, nil
	}

	value, err = mvcc.LM.Get(key, skey, mvcc.TS)
	if err != nil {
		mvcc.abort()
		return nil, err
	}
	mvcc.View[skey] = value
	return value, nil
}

func (mvcc *MVCCInstance) GetForUpdate(key []byte) (value []byte, err error) {
	return nil, nil
}

func (mvcc *MVCCInstance) Put(key []byte, value []byte) (err error) {
	if mvcc.State != TxStateRunning {
		return errors.New("tx not running")
	}

	skey := string(key)
	_, has := mvcc.Locks[skey]
	if !has {
		if !mvcc.LM.Lock(key, skey, mvcc.TS) {
			mvcc.abort()
			return errors.New("abort")
		}
		mvcc.Locks[skey] = 1
	}

	mvcc.View[skey] = value
	return nil
}

func (mvcc *MVCCInstance) Increase32(key []byte, value int32) (err error) {
	return nil
}

func (mvcc *MVCCInstance) Delete(key []byte) (err error) {
	return mvcc.Put(key, nil)
}

func (mvcc *MVCCInstance) Commit() (err error) {
	if mvcc.State != TxStateRunning {
		return errors.New("tx not running")
	}

	kvs := make([]storage.KV, len(mvcc.Locks))
	i := 0
	for k := range mvcc.Locks {
		kvs[i] = storage.KV{Key: []byte(k), Value: mvcc.View[k]}
		i += 1
	}

	mvcc.Store.Lock()
	err = mvcc.Store.PutBatch(kvs)
	mvcc.Store.Unlock()
	if err != nil {
		mvcc.abort()
		return err
	}

	for k := range mvcc.Locks {
		mvcc.LM.UpdateVersion(string(k), mvcc.View[k], mvcc.TS)
	}
	mvcc.unlockAllLocks()
	return nil
}

func (mvcc *MVCCInstance) Abort() (err error) {
	if mvcc.State != TxStateRunning {
		return errors.New("tx not running")
	}
	mvcc.abort()
	return nil
}
