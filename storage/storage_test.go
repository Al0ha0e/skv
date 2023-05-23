package storage

import (
	"bytes"
	"fmt"
	"os"
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

	buf := SerializeSingleWithHeader(kv)

	header, _ := DeserializeHeader(bytes.NewReader(buf))

	fmt.Println(header)
}

func TestDeserializeSingle(t *testing.T) {
	kv := KV{}
	kv.Key = []byte{1, 2, 3}
	kv.Value = []byte{4, 5, 114}

	buf := bytes.NewReader(SerializeSingleWithHeader(kv))

	header, err := DeserializeHeader(buf)
	fmt.Println(header)
	if err != nil {
		t.Error(err)
	}

	kv2, err := DeserializeSingle(buf, true, header.CRC)
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

func TestDeserializeMulti(t *testing.T) {
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

	kvs := make([]KV, len(keys))

	for i, k := range keys {
		kvs[i] = KV{k, values[i]}
	}

	buf := bytes.NewReader(SerializeMultiWithHeader(kvs))

	header, err := DeserializeHeader(buf)
	fmt.Println(header)
	if err != nil {
		t.Error(err)
	}

	kvs2, err := DeserializeMulti(buf, true, header.CRC)
	if err != nil {
		t.Error(err)
	}

	for i, kv := range kvs {
		kv2 := kvs2[i]
		if string(kv2.Key) != string(kv.Key) {
			t.Error("bad key", kv.Key, kv2.Key)
		}

		if string(kv2.Value) != string(kv.Value) {
			t.Error("bad key", kv.Value, kv2.Value)
		}

		fmt.Println("K", kv.Key, kv2.Key)
		fmt.Println("V", kv.Value, kv2.Value)
	}
}

func TestBitcask(t *testing.T) {
	os.Remove("../testdata/test.skv")
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
	os.Remove("../testdata/test.skv")
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

	kvs := []KV{
		{[]byte{9, 2, 3}, []byte{1, 1, 1}},
		{[]byte{9, 5, 114}, []byte{2, 2}},
		{[]byte{9, 9, 10}, []byte{3}},
	}

	for idx, k := range keys {
		store.Put(k, values[idx])
	}

	store.PutBatch(kvs)

	for _, k := range keys {
		v, _ := store.Get(k)
		fmt.Println(v)
	}

	store.Delete(keys[2])

	store.Close()

	fmt.Println("--------------")

	store, _ = Open("../testdata/test.skv")

	for _, k := range keys {
		v, _ := store.Get(k)
		fmt.Println(v)
	}

	for _, kv := range kvs {
		v, _ := store.Get(kv.Key)
		fmt.Println(v)
	}
}
