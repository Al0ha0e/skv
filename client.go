package skv

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

type TesterClient struct {
	url  string
	conn net.Conn
}

func MakeTestClient(url string) *TesterClient {
	return &TesterClient{
		url: url,
	}
}

func (tc *TesterClient) SendPacked(pack Operation) error {
	buf := &bytes.Buffer{}
	enc := gob.NewEncoder(buf)
	enc.Encode(pack)
	_, err := tc.conn.Write(buf.Bytes())
	if err != nil {
		return err
	}

	return nil
}

func (tc *TesterClient) Send(op OPType, key string, value int32) error {
	pack := Operation{
		OP:    op,
		Key:   key,
		Value: value,
	}
	return tc.SendPacked(pack)
}

func (tc *TesterClient) Recv() OperationResult {
	pack := OperationResult{}
	binary.Read(tc.conn, binary.BigEndian, &pack)
	return pack
}

func (tc *TesterClient) OperatePacked(pack Operation) OperationResult {
	tc.SendPacked(pack)
	return tc.Recv()
}

func (tc *TesterClient) Operate(op OPType, key string, value int32) OperationResult {
	tc.Send(op, key, value)
	return tc.Recv()
}

func (tc *TesterClient) Run() error {
	conn, err := net.Dial("tcp", tc.url)
	if err != nil {
		return err
	}
	tc.conn = conn
	// defer tc.conn.Close()

	// for i := 0; i < 1000; i++ {
	// 	tc.send(OPINC, int32(i), 514)
	// }

	return nil
}

func (tc *TesterClient) Stop() {
	tc.conn.Close()
	tc.conn = nil
}

func ParseCase(path string) []Operation {
	ret := make([]Operation, 0)

	file, err := os.Open(path)
	if err != nil {
		return ret
	}

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 {
			continue
		}
		fields := strings.Fields(line)
		op := fields[0]
		var opi OPType
		var key string
		value := int32(0)

		if op == "BEGIN" {
			opi = OPTXSTART
		} else if op == "COMMIT" {
			opi = OPCOMMIT
		} else if op == "ABORT" {
			opi = OPABORT
		} else {
			key = fields[1]
			if op == "GET" {
				opi = OPGET
			} else if op == "PUT" {
				f := fields[2]
				if f[0] == '(' {
					opi = OPINC

					for i := 1; i < len(f)-1; i++ {
						if f[i] == '+' {
							v, _ := strconv.ParseInt(f[i+1:len(f)-1], 10, 32)
							value = int32(v)
							break
						} else if f[i] == '-' {
							v, _ := strconv.ParseInt(f[i+1:len(f)-1], 10, 32)
							value = int32(-v)
							break
						}
					}
				} else {
					opi = OPPUT
					v, _ := strconv.ParseInt(f, 10, 32)
					value = int32(v)
				}
			} else {
				opi = OPDEL
			}
		}

		sb := Operation{
			OP:    opi,
			Key:   key,
			Value: value,
		}
		fmt.Println(sb)
		ret = append(ret, sb)
	}
	file.Close()
	return ret
}
