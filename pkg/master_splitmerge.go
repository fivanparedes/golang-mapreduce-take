package pkg

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
)

// merge combines the results of the many reduce jobs into a single output file
// XXX use merge sort
func (mr *Master) merge() {
	Debug("Merge phase")
	kvs := make(map[string]string)
	for i := 0; i < mr.nReduce; i++ {
		p := mergeName(mr.jobName, i)
		Debug("Merge: read %s\n", p)
		file, err := os.Open(p)
		if err != nil {
			log.Fatal("Merge: ", err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err = dec.Decode(&kv)
			if err != nil {
				break
			}
			kvs[kv.Key] = kv.Value
		}
		file.Close()
	}
	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	file, err := os.Create("mrtmp." + mr.jobName)
	if err != nil {
		log.Fatal("Merge: create ", err)
	}
	w := bufio.NewWriter(file)
	for _, k := range keys {
		fmt.Fprintf(w, "%s: %s\n", k, kvs[k])
	}
	w.Flush()
	file.Close()
}

// removeFile is a simple wrapper around os.Remove that logs errors.
func RemoveFile(n string) {
	err := os.Remove(n)
	if err != nil {
		log.Fatal("CleanupFiles ", err)
	}
}

// CleanupFiles removes all intermediate files produced by running mapreduce.
func (mr *Master) CleanupFiles() {
	for i := range mr.Files {
		for j := 0; j < mr.nReduce; j++ {
			RemoveFile(reduceName(mr.jobName, i, j))
		}
	}
	for i := 0; i < mr.nReduce; i++ {
		RemoveFile(mergeName(mr.jobName, i))
	}
	RemoveFile("mrtmp." + mr.jobName)
}
