package storage

import (
	"bytes"
	"fmt"
	"testing"
)

func TestSerializeSingle(t *testing.T) {
	kv := KV{}
	kv.Key = []byte{1, 2, 3}
	kv.Value = nil

	buf := SerializeSingle(kv)
	fmt.Println(buf)
}

func TestDeserializeHeader(t *testing.T) {
	kv := KV{}
	kv.Key = []byte{1, 2, 3}
	kv.Value = []byte{4, 5, 114}

	buf := SerializeSingle(kv)

	header, _ := DeserializeHeader(bytes.NewReader(buf))

	fmt.Println(header)
}

func TestDeserializeSingle(t *testing.T) {
	kv := KV{}
	kv.Key = []byte{1, 2, 3}
	kv.Value = []byte{4, 5, 114}

	buf := bytes.NewReader(SerializeSingle(kv))

	header, err := DeserializeHeader(buf)
	fmt.Println(header)
	if err != nil {
		t.Error(err)
	}

	kv2, err := DeserializeSingle(buf, header.CRC)
	if err != nil {
		t.Error(err)
	}

	if string(kv2.Key) != string(kv.Key) {
		t.Error("bad key", kv.Key, kv2.Key)
	}

	if string(kv2.Value) != string(kv.Value) {
		t.Error("bad key", kv.Value, kv2.Value)
	}

	fmt.Println(kv.Key, kv2.Key)
	fmt.Println(kv.Value, kv2.Value)
}

func TestBitcask(t *testing.T) {
	store, err := Open("../testdata/test.skv")

	if err != nil {
		t.Fatal(err)
	}

	key := []byte{1, 2, 3}
	value := []byte{4, 5, 114}

	v, _ := store.Get(key)
	fmt.Println(v)

	err = store.Put(key, value)
	if err != nil {
		t.Fatal(err)
	}

	v, _ = store.Get(key)
	fmt.Println(v)

	err = store.Delete(key)
	if err != nil {
		t.Fatal(err)
	}

	v, _ = store.Get(key)
	fmt.Println(v)
}

func TestBitcaskLoad(t *testing.T) {
	store, err := Open("../testdata/test.skv")

	if err != nil {
		t.Fatal(err)
	}

	keys := [][]byte{
		{1, 2, 3},
		{4, 5, 114},
		{8, 9, 10},
	}

	values := [][]byte{
		{1, 5, 3},
		{5, 1, 4},
		{4, 1, 7},
	}

	for idx, k := range keys {
		store.Put(k, values[idx])
	}

	for _, k := range keys {
		v, _ := store.Get(k)
		fmt.Println(v)
	}

	store.Delete(keys[2])

	store.Close()

	store, _ = Open("../testdata/test.skv")

	for _, k := range keys {
		v, _ := store.Get(k)
		fmt.Println(v)
	}
}
