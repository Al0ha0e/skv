package transaction

import (
	"testing"

	"github.com/Al0ha0e/skv/storage"
)

func TestLock(t *testing.T) {
	lm := MakeLockManager()
	keys := []string{"aaa", "bbb"}
	ok := lm.Lock(keys[0], true)
	t.Log("---", ok, lm.Locks[keys[0]])
	ok = lm.Lock(keys[0], false)
	t.Log("---", ok, lm.Locks[keys[0]])
	ok = lm.Lock(keys[0], true)
	t.Log("---", ok, lm.Locks[keys[0]])
	lm.Unlock(keys[0])
	t.Log("---", ok, lm.Locks[keys[0]])
	lm.Unlock(keys[0])
	t.Log("---", ok, lm.Locks[keys[0]])
}

func TestPut(t *testing.T) {
	sb := []byte{1, 2, 3}
	sb1 := string(sb)
	sb2 := []byte(sb1)
	t.Log(sb[0] == sb2[0])
	t.Log(sb[1] == sb2[1])
	t.Log(sb[2] == sb2[2])
	t.Log("----------")

	lm := MakeLockManager()
	store := storage.MakeNaiveStorage()
	instance := lm.MakeTwoPLInstance(store)
	instance.Put([]byte{1, 2, 3}, []byte{11, 22, 33})
	t.Log(instance.Get([]byte{1, 2, 3}))
	t.Log(instance.Get([]byte{4, 5, 6}))
	instance.Commit()
	instance2 := lm.MakeTwoPLInstance(store)
	t.Log(instance2.Get([]byte{1, 2, 3}))
	t.Log(instance2.Put([]byte{1, 2, 3}, []byte{44, 55, 66}))
	t.Log(instance2.Get([]byte{1, 2, 3}))
	instance3 := lm.MakeTwoPLInstance(store)
	t.Log(instance3.Get([]byte{1, 2, 3}))
	t.Log(instance3.Put([]byte{1, 2, 3}, []byte{77, 88, 99}))
}

func TestDelete(t *testing.T) {
	lm := MakeLockManager()
	store := storage.MakeNaiveStorage()
	instance := lm.MakeTwoPLInstance(store)
	instance.Put([]byte{1, 2, 3}, []byte{11, 22, 33})
	t.Log(instance.Get([]byte{1, 2, 3}))
	instance.Commit()
	instance2 := lm.MakeTwoPLInstance(store)
	t.Log(instance2.Get([]byte{1, 2, 3}))
	t.Log(instance2.Delete([]byte{1, 2, 3}))
	t.Log(instance2.Get([]byte{1, 2, 3}))
	instance2.Commit()
	instance3 := lm.MakeTwoPLInstance(store)
	t.Log(instance3.Get([]byte{1, 2, 3}))

}
