package mapreduce


import (
    "fmt"
    "os"
    "encoding/json"
    "log"
    "sort"
    //"bufio"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
    fmt.Printf("doReduce: jobName: %s, reduceTask: %d, nMap: %d, outFile %s\n",
            jobName, reduceTask, nMap, outFile)
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

    // read temp file first
    var kvs = make(map[string][]string)
    var fileNames = make([]*os.File, nMap)
    
    var akv KeyValue
    var keysForSort []string
    // tempFileName := fmt.Sprintf("mrtmp.test-res-%d-%d", nMap, reduceTask)
    for fileCount := 0; fileCount < nMap; fileCount++ {
        tempFileName := reduceName(jobName, fileCount, reduceTask)
        aTempFiles, err := os.Open(tempFileName) 
        fileNames[fileCount] = aTempFiles
        defer fileNames[fileCount].Close()
        if err != nil {
            fmt.Printf("open file %s failed!\n", tempFileName)
        }

        enc := json.NewDecoder(aTempFiles)
        for  {
            err := enc.Decode(&akv)
            // fmt.Println("decoder")
            // fmt.Println(akv)
            // kvs = append(kvs, amap)
            kvs[akv.Key] = append(kvs[akv.Key], akv.Value)
            keysForSort = append(keysForSort, akv.Key)
            if err != nil {
                break
            }
        }
    }
    //fmt.Println(kvs)
    
    // sort
    sort.Strings(keysForSort)
    //fmt.Println("doReduce finished")

    // write input file by current reducer!
    // name := fmt.Sprintf("mrtmp.test-res-%d", reduceTask)
    outputFile, err := os.Create(outFile)
    if err != nil {
        log.Fatalf("2 file %s not exites!\n", outFile)
    }
    defer outputFile.Close()
    // write result contents
    enc2 := json.NewEncoder(outputFile)
    for _, key := range keysForSort {
        values := reduceF(key, kvs[key])
        akv.Key = key
        akv.Value = values
        enc2.Encode(&akv)
    }
}
