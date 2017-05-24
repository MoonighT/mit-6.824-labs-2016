package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
)

// doMap reads the assigned input file, calls mapF for that file's contents,
// and writes the output into nReduce output bins.
func doMap(
	jobName string,
	job int,
	inFile string,
	nReduce int,
	mapF func(string, string) []KeyValue,
) {
	// TODO:
	// You will need to write this function.
	// You can find the output bin filename for reduce job r using reduceName(jobName, job, r).
	// The ihash function (given below doMap) should be used for output binning.
	//
	// You may choose how you would like to encode the key/value pairs in
	// the intermediate files. One way would be to encode them using JSON.
	// Since the reduce jobs have to output JSON anyway, you should
	// probably do that here too. You can write JSON to a file using
	//
	//   enc := json.NewEncode(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!
	file, err := os.Open(inFile)
	if err != nil {
		log.Fatal("open in file error")
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatal("read file error")
	}
	kvs := mapF(inFile, string(content))
	// prepare files
	encMap := make([]*json.Encoder, nReduce)
	for j := 0; j < nReduce; j++ {
		name := reduceName(jobName, job, j)
		f, err := os.Create(name)
		if err != nil {
			log.Fatalf("create file error %s", err)
			return
		}
		defer f.Close()
		enc := json.NewEncoder(f)
		encMap[j] = enc
	}

	for _, kv := range kvs {
		enc := encMap[int(ihash(kv.Key))%nReduce]
		err := enc.Encode(kv)
		if err != nil {
			log.Printf("warning enc encode error %s\n", err)
		}
	}

}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
