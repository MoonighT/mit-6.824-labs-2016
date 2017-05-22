package mapreduce

import (
	"encoding/json"
	"io"
	"log"
	"os"
	"sort"
)

// doReduce reads the map outputs for the bin assigned to this job, sorts the
// outputs by key, calls reduceF for each key, and writes the output to disk.
func doReduce(
	jobName string,
	job int,
	nMap int,
	reduceF func(string, []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the output bin for this reduce job from map job m using reduceName(jobName, m, job).
	// Remember that the values in the input files are encoded, so you will
	// need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly
	// calling .Decode() on it until Decode() returns an error..
	//
	// You should write the reduced output in sorted key order as JSON
	// encoded KeyValue objects to a file named mergeName(jobName, job).
	// It will look something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	kvmap := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		// map result file name for this reducer
		name := reduceName(jobName, i, job)
		f, err := os.Open(name)
		if err != nil {
			log.Fatalf("open map file error %s\n", err)
		}
		defer f.Close()
		dec := json.NewDecoder(f)
		for {
			kv := &KeyValue{}
			err := dec.Decode(kv)
			if err == io.EOF {
				break
			}
			if _, ok := kvmap[kv.Key]; !ok {
				kvmap[kv.Key] = make([]string, 0)
			}
			kvmap[kv.Key] = append(kvmap[kv.Key], kv.Value)
		}
	}
	var keys []string
	for k, _ := range kvmap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	mName := mergeName(jobName, job)
	f, err := os.Create(mName)
	if err != nil {
		log.Fatalf("create file error %s", err)
		return
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	for _, key := range keys {
		res := reduceF(key, kvmap[key])
		err := enc.Encode(KeyValue{Key: key, Value: res})
		if err != nil {
			log.Printf("warning enc encode error %s\n", err)
		}
	}
}
