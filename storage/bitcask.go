package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"time"

	"github.com/Al0ha0e/skv/index"
)

type StorageHeader struct {
	CRC       uint32
	Timestamp int64
	Info      byte
}

func SerializeSingle(kv KV) []byte {
	kSize := uint32(len(kv.Key))
	vSize := uint32(len(kv.Value))

	var payload bytes.Buffer
	binary.Write(&payload, binary.BigEndian, kSize)
	binary.Write(&payload, binary.BigEndian, vSize)
	payload.Write(kv.Key)
	payload.Write(kv.Value)

	return payload.Bytes()
}

func SerializeSingleWithHeader(kv KV) []byte {
	payload := SerializeSingle(kv)

	var header StorageHeader
	header.CRC = crc32.ChecksumIEEE(payload)
	header.Timestamp = time.Now().UnixNano()
	header.Info = 0

	var ret bytes.Buffer
	binary.Write(&ret, binary.BigEndian, &header)
	ret.Write(payload)

	return ret.Bytes()
}

func SerializeMulti(kvs []KV) []byte {
	var payload bytes.Buffer
	binary.Write(&payload, binary.BigEndian, uint32(len(kvs)))

	for _, kv := range kvs {
		payload.Write(SerializeSingle(kv))
	}

	return payload.Bytes()
}

func SerializeMultiWithHeader(kvs []KV) []byte {

	payload := SerializeMulti(kvs)

	var header StorageHeader
	header.CRC = crc32.ChecksumIEEE(payload)
	header.Timestamp = time.Now().UnixNano()
	header.Info = 1

	var ret bytes.Buffer
	binary.Write(&ret, binary.BigEndian, &header)
	ret.Write(payload)

	return ret.Bytes()
}

func DeserializeHeader(buf io.Reader) (header StorageHeader, err error) {
	err = binary.Read(buf, binary.BigEndian, &header)
	return header, err
}

func DeserializeSingle(buf io.Reader, check bool, crc uint32) (kv KV, err error) {
	var kSize uint32
	var vSize uint32

	err = binary.Read(buf, binary.BigEndian, &kSize)
	if err != nil {
		return kv, err
	}
	err = binary.Read(buf, binary.BigEndian, &vSize)
	if err != nil {
		return kv, err
	}

	key := make([]byte, kSize)
	value := make([]byte, vSize)

	n, err := buf.Read(key)
	if err != nil {
		return kv, err
	}
	if n != int(kSize) {
		return kv, errors.New("key error")
	}

	n, err = buf.Read(value)
	if err != nil {
		return kv, err
	}
	if n != int(vSize) {
		return kv, errors.New("value error")
	}

	if check {
		payload := SerializeSingle(KV{key, value})

		if crc32.ChecksumIEEE(payload) != crc {
			return kv, errors.New("CRC mismatch")
		}
	}

	kv = KV{
		Key:   key,
		Value: value,
	}

	return kv, nil
}

func DeserializeMulti(buf io.Reader, check bool, crc uint32) (kvs []KV, err error) {
	kvs = make([]KV, 0)

	var cnt uint32
	err = binary.Read(buf, binary.BigEndian, &cnt)
	if err != nil {
		return kvs, err
	}

	for i := uint32(0); i < cnt; i++ {
		kv, err := DeserializeSingle(buf, false, 0)
		if err != nil {
			return kvs, err
		}
		kvs = append(kvs, kv)
	}

	if check {
		payload := SerializeMulti(kvs)
		if crc32.ChecksumIEEE(payload) != crc {
			return kvs, errors.New("CRC mismatch")
		}
	}

	return kvs, nil
}

func Deserialize(buf io.Reader) (kvs []KV, err error) {
	// fmt.Println("------------START DES--------------")
	kvs = make([]KV, 0)

	for {
		header, err := DeserializeHeader(buf)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, errors.New("deserialize fail, " + err.Error())
		} else {
			if header.Info == 0 {
				//single
				kv, err := DeserializeSingle(buf, true, header.CRC)
				if err != nil {
					return nil, errors.New("deserialize fail, " + err.Error())
				}
				kvs = append(kvs, kv)
				// fmt.Println("ITEM", header, kv)
			} else {
				//multi
				mkv, err := DeserializeMulti(buf, true, header.CRC)
				// for _, kv := range mkv {
				// 	fmt.Println("MULT", header, kv)
				// }
				if err != nil {
					return nil, errors.New("deserialize fail, " + err.Error())
				}
				kvs = append(kvs, mkv...)
			}
		}
	}
	// fmt.Println("------------DES OK--------------")

	return kvs, nil
}

type BitcaskStorage struct {
	index index.Index
	file  *os.File
}

func Open(path string) (store *BitcaskStorage, err error) {

	rfile, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0777)
	if err != nil {
		return nil, err
	}

	kvs, err := Deserialize(rfile)
	if err != nil {
		return nil, err
	}

	rfile.Close()

	idx := index.GetNaiveIndex()

	for _, kv := range kvs {
		v := kv.Value
		if v == nil {
			idx.Delete(kv.Key)
		} else {
			idx.Put(kv.Key, v)
		}

	}

	wfile, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE|os.O_SYNC, 0777)

	store = &BitcaskStorage{
		index: idx,   //TODO
		file:  wfile, //TODO multifile
	}

	return store, err
}

func (store *BitcaskStorage) Get(key []byte) (value []byte, err error) {
	return store.index.Get(key), nil
}

func (store *BitcaskStorage) Put(key []byte, value []byte) (err error) {
	data := SerializeSingleWithHeader(KV{key, value})

	_, err = store.file.Write(data)
	if err != nil {
		return err
	}

	store.index.Put(key, value)
	return nil
}

func (store *BitcaskStorage) PutBatch(kvs []KV) (err error) {
	//TODO compact
	data := SerializeMultiWithHeader(kvs)

	_, err = store.file.Write(data)
	if err != nil {
		return err
	}

	for _, kv := range kvs {
		store.index.Put(kv.Key, kv.Value)
	}
	return nil
}

func (store *BitcaskStorage) Delete(key []byte) (err error) {
	err = store.Put(key, nil)
	if err != nil {
		return err
	}
	store.index.Delete(key)
	return nil
}

func (store *BitcaskStorage) Close() (err error) {
	return store.file.Close()
}
