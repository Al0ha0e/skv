package index

import (
	"fmt"
	"testing"
)

func TestNaiveIndex(t *testing.T) {
	index := GetNaiveIndex()

	k := []byte{1, 2, 3}

	fmt.Println(index.Has(k), index.Get(k))
	index.Put(k, []byte{4, 5, 6})
	fmt.Println(index.Has(k), index.Get(k))
	index.Delete(k)
	fmt.Println(index.Has(k), index.Get(k))
}
