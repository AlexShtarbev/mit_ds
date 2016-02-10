package mapreduce

import (
	"os"
	"log"
	"encoding/json"
	"sort"
	"io"
	"strconv"
	"strings"
)

type KeyValueStore []KeyValue

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()

	// FIXME
	output_x, _ := os.Create("output.txt")
	output, err := os.Create("output" + strconv.Itoa(reduceTaskNumber) + ".txt")
	if(err != nil) {
		log.Fatal("could not open file : %s", output)
	}
	output.WriteString("reduce task number = " + strconv.Itoa(reduceTaskNumber) + "\n")
	var s []string

	for i := 0; i < nMap; i++ {
		reduceTaskFileName := reduceName(jobName, i, reduceTaskNumber);
		mergeFileName := mergeName(jobName, reduceTaskNumber)

		output.WriteString(reduceTaskFileName + "\n")// FIXME
		keyValues, kvErr := decode(reduceTaskFileName)

		if kvErr != nil {
			log.Fatal("could not decode file : ", reduceTaskFileName, "\nerror = ", kvErr.Error())
		}

		var keys []string

		for k, _ := range keyValues {
			keys = append(keys, k)
			s = append(s, k)
		}
		sort.Strings(keys)

		//for _, k := range keys {
		//	output.WriteString(k + "\n")// FIXME
		//}

		var file *os.File
		if _, err := os.Stat(mergeFileName); os.IsNotExist(err) {
			inFile, err := os.Create(mergeFileName)
			if(err != nil) {
				log.Fatal("could not create file : %s", mergeFileName)
			}
			file = inFile
		} else {
			inFile, err := os.OpenFile(mergeFileName, os.O_APPEND|os.O_WRONLY, 0600)
			if(err != nil) {
				log.Fatal("could not open file : %s", mergeFileName)
			}
			file = inFile
		}

		enc := json.NewEncoder(file)
		for _, k := range keys{
			//output.WriteString(k + "\n")// FIXME
			res := reduceF(k, keyValues[k])
			kv := KeyValue{k, res}
			err := enc.Encode(&kv)
			if(err != nil) {
				//log.Fatal("could not marshall key/value pair in file %s", file.Name())
			}
		}

		file.Close()
	}

	sort.Strings(s)
	output_x.WriteString(strings.Join(s, "\n"))
	output.Close() //FIXME
}

func decode(fileName string) (map[string][]string, error) {
	file, err := os.Open(fileName)
	if (err != nil) {
		log.Fatal("could not open file : %s", fileName)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)

	kvs := make(map[string][]string)

	for {
		var kv KeyValue
		err := decoder.Decode(&kv)
		if err != nil {
			if (err != io.EOF) {
				return nil, err
			}
			break;
		}

		kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
	}

	return kvs, nil
}
