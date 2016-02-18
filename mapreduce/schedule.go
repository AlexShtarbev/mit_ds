package mapreduce

import (
	"fmt"
)

type Reply struct {
	OK bool
}

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)
	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//

	taskChannel := make(chan *DoTaskArgs)
	doneChannel := make(chan bool)
	provideNextWorker := func () string{
		var worker string
		worker =<- mr.registerChannel

		return worker
	}

	runTask := func(worker string, task *DoTaskArgs) {
		var reply Reply
		ok := call(worker, "Worker.DoTask", task, &reply)
		if ok {
			doneChannel <- true
			mr.registerChannel <- worker
		} else {
			taskChannel <- task
		}
	}

	go func() {
		for task := range taskChannel {
			worker := provideNextWorker()
			go func(task *DoTaskArgs) {
				runTask(worker,task)
			}(task)
		}
	}()

	go func() {
		for i:= 0; i < ntasks; i++ {
			task := DoTaskArgs{mr.jobName, mr.files[i], phase, i, nios}
			taskChannel <- &task
		}
	}()


	for i:=0; i < ntasks; i++ {
		<- doneChannel
	}

	close(taskChannel)

	fmt.Printf("Schedule: %v phase done\n", phase)
}

