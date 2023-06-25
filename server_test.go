package skv

import (
	"os"
	"sync"
	"testing"
	"time"
)

func TestParseCase(t *testing.T) {
	ParseCase("./testdata/cases/1.txt")
}

func TestServer(t *testing.T) {

	server, err := MakeTestServer("./testdata/test.skv", "127.0.0.1:20000")
	if err != nil {
		t.Error(err)
	}

	go func() {
		server.Run()
	}()

	client := MakeTestClient("127.0.0.1:20000")

	client.Run()
	// client.Operate(OPPUT, "A", 3)
	// client.Operate(OPPUT, "B", 4)

	// client.Operate(OPINC, "A", 1)
	// client.Operate(OPINC, "B", 1)

	// t.Log(client.Operate(OPGET, "A", 0))
	// t.Log(client.Operate(OPGET, "B", 0))
	// client.Operate(OPDEL, "A", 0)
	// client.Operate(OPDEL, "B", 0)
	// client.Operate(OPPUT, "A", 5)
	// t.Log(client.Operate(OPGET, "A", 0))
	// t.Log(client.Operate(OPGET, "B", 0))
	// client.Operate(OPPUT, "B", 5)
	// t.Log(client.Operate(OPGET, "B", 0))
	t.Log(client.Operate(OPGET, "A", 0))
	t.Log(client.Operate(OPGET, "B", 0))
}

func TestCase1(t *testing.T) {
	os.Remove("./testdata/test.skv")
	server, err := MakeTestServer("./testdata/test.skv", "127.0.0.1:20000")
	if err != nil {
		t.Error(err)
	}

	go func() {
		server.Run()
	}()

	ops := ParseCase("./testdata/cases/1.txt")

	client := MakeTestClient("127.0.0.1:20000")
	client.Run()

	for _, op := range ops {
		t.Log(op, client.OperatePacked(op))
	}

	client.Stop()
}

func TestCase2(t *testing.T) {
	os.Remove("./testdata/test.skv")
	server, err := MakeTestServer("./testdata/test.skv", "127.0.0.1:20000")
	if err != nil {
		t.Error(err)
	}

	go func() {
		server.Run()
	}()

	ops1 := ParseCase("./testdata/cases/1.txt")
	ops2 := ParseCase("./testdata/cases/2.txt")

	client := MakeTestClient("127.0.0.1:20000")
	client.Run()

	for _, op := range ops1 {
		t.Log(op, client.OperatePacked(op))
	}

	client.Stop()
	server.Stop()

	server, err = MakeTestServer("./testdata/test.skv", "127.0.0.1:20000")
	if err != nil {
		t.Error(err)
	}
	go func() {
		server.Run()
	}()

	client.Run()

	for _, op := range ops2 {
		t.Log(op, client.OperatePacked(op))
	}
}

func TestCase3(t *testing.T) {
	os.Remove("./testdata/test.skv")
	server, err := MakeTestServer("./testdata/test.skv", "127.0.0.1:20000")
	if err != nil {
		t.Error(err)
	}

	go func() {
		server.Run()
	}()

	paths := []string{
		"./testdata/cases/3_1.txt",
		"./testdata/cases/3_2.txt",
		"./testdata/cases/3_3.txt"}

	var wg sync.WaitGroup

	task := func(ops []Operation) {
		client := MakeTestClient("127.0.0.1:20000")
		client.Run()
		for i := 0; i < 10; i += 1 {
			for _, op := range ops {
				t.Log(op, client.OperatePacked(op))
			}
		}
		client.Stop()
		wg.Done()
	}

	for i := 0; i < 3; i++ {
		ops := ParseCase(paths[i])
		wg.Add(1)
		go task(ops)
	}

	wg.Wait()
	server.Stop()
}

func runTXTask(t *testing.T, wg *sync.WaitGroup, ops []Operation) {
	client := MakeTestClient("127.0.0.1:20000")
	client.Run()

	ch1 := make(chan int, 1)
	ch1 <- 1
	timer := time.NewTimer(time.Minute)
	ticker := time.NewTicker(5 * time.Second)
	for {
		done := false
		select {
		case <-ch1:
			ch1 <- 1
			badTx := false
			for _, op := range ops {
				if !badTx {
					res := client.OperatePacked(op)
					if res.State&2 == 0 {
						badTx = true
						// t.Log("ABORTED")
					}
				} else {
					if op.OP == OPABORT || op.OP == OPCOMMIT {
						badTx = false
					}
				}
			}
		case tm := <-ticker.C:
			t.Log(tm)
		case <-timer.C:
			done = true
		}
		if done {
			ticker.Stop()
			break
		}
	}

	client.Stop()
	wg.Done()
}

func TestCase4(t *testing.T) {
	os.Remove("./testdata/test.skv")

	server, err := MakeTestServer("./testdata/test.skv", "127.0.0.1:20000")
	if err != nil {
		t.Error(err)
	}

	go func() {
		server.Run()
	}()

	client := MakeTestClient("127.0.0.1:20000")

	ops1 := ParseCase("./testdata/cases/4_1.txt")
	ops2 := ParseCase("./testdata/cases/4_2.txt")
	ops3 := ParseCase("./testdata/cases/4_3.txt")

	client.Run()
	for _, op := range ops1 {
		t.Log(op, client.OperatePacked(op))
	}
	client.Stop()

	var wg sync.WaitGroup

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go runTXTask(t, &wg, ops2)
	}

	t.Log("OK1")
	wg.Wait()
	t.Log("OK2")

	server.Stop()
	server, err = MakeTestServer("./testdata/test.skv", "127.0.0.1:20000")
	if err != nil {
		t.Error(err)
	}

	go func() {
		server.Run()
	}()

	client.Run()
	for _, op := range ops3 {
		t.Log(op, client.OperatePacked(op))
	}
	client.Stop()

}

func TestCase5(t *testing.T) {
	os.Remove("./testdata/test.skv")
	server, err := MakeTestServer("./testdata/test.skv", "127.0.0.1:20000")
	if err != nil {
		t.Error(err)
	}

	go func() {
		server.Run()
	}()

	client := MakeTestClient("127.0.0.1:20000")

	ops1 := ParseCase("./testdata/cases/5_1.txt")
	ops2 := ParseCase("./testdata/cases/5_2.txt")
	ops3 := ParseCase("./testdata/cases/5_3.txt")
	ops4 := ParseCase("./testdata/cases/5_4.txt")

	client.Run()
	for _, op := range ops1 {
		t.Log(op, client.OperatePacked(op))
	}
	client.Stop()

	var wg sync.WaitGroup

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go runTXTask(t, &wg, ops2)
	}

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go runTXTask(t, &wg, ops3)
	}

	t.Log("OK1")
	wg.Wait()
	t.Log("OK2")

	server.Stop()
	server, err = MakeTestServer("./testdata/test.skv", "127.0.0.1:20000")
	if err != nil {
		t.Error(err)
	}

	go func() {
		server.Run()
	}()

	client.Run()
	for _, op := range ops4 {
		t.Log(op, client.OperatePacked(op))
	}
	client.Stop()
}
