package mapreduce

import (
	"fmt"
	"sync"
	"log"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
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

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	var wg sync.WaitGroup
	var taskArgs DoTaskArgs
	taskArgs.JobName = jobName
	taskArgs.Phase = phase
	taskArgs.NumOtherPhase = n_other
	// assign n tasks to workers to execute the function
	for nLeftTasks := ntasks ;nLeftTasks > 0; nLeftTasks-- {
		numTask := ntasks - nLeftTasks
		if phase == mapPhase {
			taskArgs.File = mapFiles[numTask]
		}
		taskArgs.TaskNumber = numTask
		wg.Add(1)
		workerAddress := <- registerChan
		go func(workerAddress string, args DoTaskArgs) {
			defer wg.Done()
			for {
				if call(workerAddress, "Worker.DoTask", &args, nil) == true {
					go func() {
						registerChan <- workerAddress
					}()
					break
				} else {
					log.Printf(
						"Schedule fail: %v %v tasks to %v\n", ntasks, phase, workerAddress)
					workerAddress = <- registerChan
				}
			}
		}(workerAddress, taskArgs)
	}
	// wait all workers done
	wg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}
