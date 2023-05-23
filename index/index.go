package index

type Index interface {
	Get(key []byte) (value []byte)
	Has(key []byte) bool
	Put(key []byte, value []byte)
	Delete(key []byte)
}

type NaiveIndex struct {
	kvs map[string][]byte
}

func GetNaiveIndex() *NaiveIndex {
	return &NaiveIndex{
		kvs: make(map[string][]byte),
	}
}

func (index *NaiveIndex) Get(key []byte) (value []byte) {
	v := index.kvs[string(key)]
	return v
}

func (index *NaiveIndex) Has(key []byte) bool {
	_, has := index.kvs[string(key)]
	return has
}

func (index *NaiveIndex) Put(key []byte, value []byte) {
	index.kvs[string(key)] = value
}

func (index *NaiveIndex) Delete(key []byte) {
	delete(index.kvs, string(key))
}
