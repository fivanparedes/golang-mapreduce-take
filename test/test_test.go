package test

import (
	mapreduce "laboratorio-4/pkg"
	"bufio"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	nNumber = 100000
	nMap    = 100
	nReduce = 50
)

// Create input file with N numbers
// Check if we have N numbers in output file

// Split in words
func MapFunc(file string, value string) (res []mapreduce.KeyValue) {
	mapreduce.Debug("Map %v\n", value)
	words := strings.Fields(value)
	for _, w := range words {
		kv := mapreduce.KeyValue{Key: w, Value: ""}
		res = append(res, kv)
	}
	return
}

// Just return key
func ReduceFunc(key string, values []string) string {
	for _, e := range values {
		mapreduce.Debug("Reduce %s %v\n", key, e)
	}
	return ""
}

// Checks input file agaist output file: each input number should show up
// in the output file in string sorted order
func check(t *testing.T, files []string) {
	output, err := os.Open("mrtmp.test")
	if err != nil {
		log.Fatal("check: ", err)
	}
	defer output.Close()

	var lines []string
	for _, f := range files {
		input, err := os.Open(f)
		if err != nil {
			log.Fatal("check: ", err)
		}
		defer input.Close()
		inputScanner := bufio.NewScanner(input)
		for inputScanner.Scan() {
			lines = append(lines, inputScanner.Text())
		}
	}

	sort.Strings(lines)

	outputScanner := bufio.NewScanner(output)
	i := 0
	for outputScanner.Scan() {
		var v1 int
		var v2 int
		text := outputScanner.Text()
		n, err := fmt.Sscanf(lines[i], "%d", &v1)
		if n == 1 && err == nil {
			_, err = fmt.Sscanf(text, "%d", &v2)
		}
		if err != nil || v1 != v2 {
			t.Fatalf("line %d: %d != %d err %v\n", i, v1, v2, err)
		}
		i++
	}
	if i != nNumber {
		t.Fatalf("Expected %d lines in output\n", nNumber)
	}
}

// Workers report back how many RPCs they have processed in the Shutdown reply.
// Check that they processed at least 1 RPC.
func checkWorker(t *testing.T, l []int) {
	for _, tasks := range l {
		if tasks == 0 {
			t.Fatalf("Some worker didn't do any work\n")
		}
	}
}

// Make input file
func makeInputs(num int) []string {
	var names []string
	var i = 0
	for f := 0; f < num; f++ {
		names = append(names, fmt.Sprintf("824-mrinput-%d.txt", f))
		file, err := os.Create(names[f])
		if err != nil {
			log.Fatal("mkInput: ", err)
		}
		w := bufio.NewWriter(file)
		for i < (f+1)*(nNumber/num) {
			fmt.Fprintf(w, "%d\n", i)
			i++
		}
		w.Flush()
		file.Close()
	}
	return names
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp. can't use current directory since
// AFS doesn't support UNIX-domain sockets.
func port(suffix string) string {
	s := "/var/tmp/824-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += "mr"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += suffix
	return s
}

func setup() *mapreduce.Master {
	files := makeInputs(nMap)
	master := port("master")
	mr := mapreduce.Distributed("test", files, nReduce, master)
	return mr
}

func cleanup(mr *mapreduce.Master) {
	mr.CleanupFiles()
	for _, f := range mr.Files {
		mapreduce.RemoveFile(f)
	}
}

func TestSequentialSingle(t *testing.T) {
	mr := mapreduce.Sequential("test", makeInputs(1), 1, MapFunc, ReduceFunc)
	mr.Wait()
	check(t, mr.Files)
	checkWorker(t, mr.Stats)
	cleanup(mr)
}

func TestSequentialMany(t *testing.T) {
	mr := mapreduce.Sequential("test", makeInputs(5), 3, MapFunc, ReduceFunc)
	mr.Wait()
	check(t, mr.Files)
	checkWorker(t, mr.Stats)
	cleanup(mr)
}

func TestBasic(t *testing.T) {
	mr := setup()
	for i := 0; i < 2; i++ {
		go mapreduce.RunWorker(mr.Address, port("worker"+strconv.Itoa(i)),
			MapFunc, ReduceFunc, -1)
	}
	mr.Wait()
	check(t, mr.Files)
	checkWorker(t, mr.Stats)
	cleanup(mr)
}

func TestOneFailure(t *testing.T) {
	mr := setup()
	// Start 2 workers that fail after 10 tasks
	go mapreduce.RunWorker(mr.Address, port("worker"+strconv.Itoa(0)),
		MapFunc, ReduceFunc, 10)
	go mapreduce.RunWorker(mr.Address, port("worker"+strconv.Itoa(1)),
		MapFunc, ReduceFunc, -1)
	mr.Wait()
	check(t, mr.Files)
	checkWorker(t, mr.Stats)
	cleanup(mr)
}

func TestManyFailures(t *testing.T) {
	mr := setup()
	i := 0
	done := false
	for !done {
		select {
		case done = <-mr.DoneChannel:
			check(t, mr.Files)
			cleanup(mr)
		default:
			// Start 2 workers each sec. The workers fail after 10 tasks
			w := port("worker" + strconv.Itoa(i))
			go mapreduce.RunWorker(mr.Address, w, MapFunc, ReduceFunc, 10)
			i++
			w = port("worker" + strconv.Itoa(i))
			go mapreduce.RunWorker(mr.Address, w, MapFunc, ReduceFunc, 10)
			i++
			time.Sleep(1 * time.Second)
		}
	}
}
