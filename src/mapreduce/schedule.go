package mapreduce

import ("fmt"
"sync"
"context"
// "time"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//o


func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
    var wg sync.WaitGroup
    ctx, cancel := context.WithCancel(context.Background())
    for handled := 0; handled < ntasks; handled++ {
        var aworker = ""
        var err error
        var file = ""
        var doTaskArgs DoTaskArgs

        if phase == mapPhase {
            file = mapFiles[handled]
        }
        doTaskArgs = DoTaskArgs{jobName, file, phase, handled, n_other}

        // get valid worker to handle the task
        aworker = <-registerChan

        wg.Add(1)
        go func(workers chan string, fatherCtx context.Context, handling int) {
            fmt.Printf("handle task %d\n", handled)
            childCtx, _ := context.WithCancel(fatherCtx)
            ok := call(aworker, "Worker.DoTask", doTaskArgs, err)
            for ok == false {
                fmt.Printf("call Worker.DoTask failed\n")
                // should give the task to another worker 
                aworker = <-registerChan
                ok = call(aworker, "Worker.DoTask", doTaskArgs, err)
            }
            wg.Done()
            select {
            case <-childCtx.Done() :
                fmt.Printf("ctx.Done\n")
            case registerChan <- aworker:
                fmt.Printf("add worker: %d\n", handling)
            }
            fmt.Printf("go task finish %d\n", handling)
        }(registerChan, ctx, handled)
    }
    fmt.Printf("wg wait\n")
    wg.Wait()
    fmt.Printf("main thread call cancel\n")
    cancel()
    fmt.Printf("schedule finish\n")
}
