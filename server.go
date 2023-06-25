package skv

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"net"

	"github.com/Al0ha0e/skv/transaction"
)

type OPType = int8

const (
	OPGET OPType = iota
	OPPUT
	OPDEL
	OPINC
	OPTXSTART
	OPCOMMIT
	OPABORT
)

type Operation struct {
	OP    OPType
	Key   string
	Value int32
}

func MakeOperation(op OPType, key string, value int32) Operation {
	return Operation{
		OP:    op,
		Key:   key,
		Value: value,
	}
}

type OperationResult struct {
	Value int32
	State int8
}

type TesterServer struct {
	db       *DB
	url      string
	listener net.Listener
}

func MakeTestServer(path string, url string) (*TesterServer, error) {
	db, err := Open(path)
	if err != nil {
		return nil, err
	}
	return &TesterServer{
		db:  db,
		url: url,
	}, nil
}

func (ts *TesterServer) send(conn net.Conn, value int32, state int8) error {
	pack := OperationResult{
		Value: value,
		State: state,
	}
	buf := &bytes.Buffer{}
	binary.Write(buf, binary.BigEndian, pack)
	_, err := conn.Write(buf.Bytes())
	if err != nil {
		return err
	}

	return nil
}

func (ts *TesterServer) processDirect(pack Operation) (int32, int8) {
	key := []byte(pack.Key)

	ivalue := int32(0)
	has := false

	switch pack.OP {
	case OPGET:
		value, _ := ts.db.Get(key)
		if value != nil {
			has = true
			binary.Read(bytes.NewBuffer(value), binary.BigEndian, &ivalue)
		}
	case OPPUT:
		buf := &bytes.Buffer{}
		binary.Write(buf, binary.BigEndian, pack.Value)
		ts.db.Put(key, buf.Bytes())
	case OPDEL:
		ts.db.Delete(key)
	case OPINC:
		ts.db.Increase32(key, pack.Value)
	}

	state := int8(2)
	if has {
		state += 1
	}
	return ivalue, state
}

func (ts *TesterServer) processTx(pack Operation, tx transaction.Transaction) (int32, int8) {
	key := []byte(pack.Key)

	ivalue := int32(0)
	has := false
	var err error

	switch pack.OP {
	case OPGET:
		var value []byte
		value, err = tx.Get(key)
		if value != nil {
			has = true
			binary.Read(bytes.NewBuffer(value), binary.BigEndian, &ivalue)
		}
	case OPPUT:
		buf := &bytes.Buffer{}
		binary.Write(buf, binary.BigEndian, pack.Value)
		err = tx.Put(key, buf.Bytes())
	case OPDEL:
		err = tx.Delete(key)
	case OPINC:
		err = tx.Increase32(key, pack.Value)
	case OPCOMMIT:
		err = tx.Commit()
	case OPABORT:
		err = tx.Abort()
	}

	state := int8(0)
	if has {
		state = 1
	}
	if err == nil {
		state += 2
	}
	return ivalue, state
}

func (ts *TesterServer) process(conn net.Conn) {
	defer conn.Close()
	isTx := false
	var tx transaction.Transaction
	for {
		pack := Operation{}
		decoder := gob.NewDecoder(conn)
		err := decoder.Decode(&pack)
		// err := binary.Read(conn, binary.BigEndian, &pack)
		if err != nil {
			fmt.Println(err)
			break
		}

		if pack.OP == OPTXSTART {
			isTx = true
			tx = ts.db.StartTransaction()
			ts.send(conn, 0, 2)
			continue
		}

		// fmt.Println(pack)
		var ivalue int32
		var state int8
		if isTx {
			ivalue, state = ts.processTx(pack, tx)
		} else {
			ivalue, state = ts.processDirect(pack)
		}

		if pack.OP == OPABORT || pack.OP == OPCOMMIT || state&2 == 0 {
			isTx = false
			tx = nil
		}

		ts.send(conn, ivalue, state)
	}

}

func (ts *TesterServer) Run() error {

	l, err := net.Listen("tcp", ts.url)
	ts.listener = l
	if err != nil {
		return err
	}

	for {
		conn, err := ts.listener.Accept()
		if err == nil {
			go ts.process(conn)
		}
	}
}

func (ts *TesterServer) Stop() {
	ts.listener.Close()
	ts.db.Close()
}
