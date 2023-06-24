package transaction

import (
	"errors"
	"sync"

	"github.com/Al0ha0e/skv/storage"
)

type LockInfo struct {
	Shared bool
	Cnt    int
}

func MakeLockInfo(shared bool) *LockInfo {
	return &LockInfo{
		Shared: shared,
		Cnt:    1,
	}
}

type LockManager struct {
	Locks map[string]*LockInfo
	latch sync.Mutex
}

func MakeLockManager() *LockManager {
	return &LockManager{
		Locks: make(map[string]*LockInfo),
	}
}

func (lm *LockManager) GetLockInfo(key string) *LockInfo {
	lm.latch.Lock()
	defer lm.latch.Unlock()
	return lm.Locks[key]
}

func (lm *LockManager) Lock(key string, shared bool) bool {
	lm.latch.Lock()
	defer lm.latch.Unlock()
	// defer fmt.Println(lm.Locks[key])

	info, has := lm.Locks[key]
	if has {
		if shared && info.Shared {
			info.Cnt += 1
		} else {
			return false
		}
	} else {
		lm.Locks[key] = MakeLockInfo(shared)
	}
	return true
}

func (lm *LockManager) Unlock(key string) {
	lm.latch.Lock()
	defer lm.latch.Unlock()

	info, has := lm.Locks[key]
	if has {
		info.Cnt -= 1
		if info.Cnt == 0 {
			delete(lm.Locks, key)
		}
	}
}

func (lm *LockManager) Upgrade(key string) bool {
	lm.latch.Lock()
	defer lm.latch.Unlock()

	info, has := lm.Locks[key]
	if has && info.Cnt == 1 {
		info.Shared = false
		return true
	}
	return false
}

type TwoPLInstance struct {
	LM    *LockManager
	Store storage.Storage
	View  map[string][]byte
	State TxState
}

func (lm *LockManager) MakeTwoPLInstance(store storage.Storage) *TwoPLInstance {
	return &TwoPLInstance{
		LM:    lm,
		Store: store,
		View:  make(map[string][]byte),
		State: TxStateRunning,
	}
}

func (twopl *TwoPLInstance) unlockAllLocks() {
	for key := range twopl.View {
		twopl.LM.Unlock(key)
	}
}

func (twopl *TwoPLInstance) abort() {
	if twopl.State != TxStateRunning {
		return
	}
	twopl.unlockAllLocks()
	twopl.State = TxStateAborted
	//TODO
}

func (twopl *TwoPLInstance) Get(key []byte) (value []byte, err error) {
	skey := string(key)
	value, ok := twopl.View[skey]
	if ok {
		return value, nil
	}

	if !twopl.LM.Lock(skey, true) { //No Wait
		twopl.abort()
		return nil, errors.New("lock fail, aborted")
	}

	value, err = twopl.Store.Get(key)
	if err != nil {
		twopl.abort()
		return nil, err
	}
	twopl.View[skey] = value
	return value, nil
}

func (twopl *TwoPLInstance) GetForUpdate(key []byte) (value []byte, err error) {
	skey := string(key)
	value, ok := twopl.View[skey]
	if ok {
		return value, nil
	}

	if !twopl.LM.Lock(skey, false) { //No Wait
		twopl.abort()
		return nil, errors.New("lock fail, aborted")
	}

	value, err = twopl.Store.Get(key)
	if err != nil {
		twopl.abort()
		return nil, err
	}
	twopl.View[skey] = value
	return value, nil
}

func (twopl *TwoPLInstance) Put(key []byte, value []byte) (err error) {
	skey := string(key)
	_, ok := twopl.View[skey]

	if ok {
		if twopl.LM.GetLockInfo(skey).Shared {
			if !twopl.LM.Upgrade(skey) {
				twopl.abort()
				return errors.New("upgrade fail, aborted")
			}
		}
		twopl.View[skey] = value
		return nil
	}

	if !twopl.LM.Lock(skey, false) { //No Wait
		twopl.abort()
		return errors.New("lock fail, aborted")
	}

	twopl.View[skey] = value
	return nil
}

func (twopl *TwoPLInstance) Delete(key []byte) (err error) {
	return twopl.Put(key, nil)
}

func (twopl *TwoPLInstance) Commit() (err error) {
	if twopl.State != TxStateRunning {
		return nil
	}

	kvs := make([]storage.KV, len(twopl.View))

	i := 0
	for k, v := range twopl.View {
		kvs[i] = storage.KV{Key: []byte(k), Value: v}
		i += 1
	}

	twopl.Store.Lock()
	err = twopl.Store.PutBatch(kvs)
	twopl.Store.Unlock()
	if err != nil {
		twopl.abort()
		return err
	}

	twopl.unlockAllLocks()
	twopl.State = TxStateCommitted
	return nil
}

func (twopl *TwoPLInstance) Abort() (err error) {
	twopl.abort()
	return nil
}
