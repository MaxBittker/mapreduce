package mapreduce

import "fmt"

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
	//load task channel with tasks
	tasks := make(chan int, ntasks)
	freeWorkers := make(chan string, 10000)
	doneTasks := make(chan int, ntasks+2)

	for _, worker := range mr.workers {
		freeWorkers <- worker
	}

	for t := 0; t < ntasks; t++ {
		tasks <- t
	}
	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)
	worker := ""
	currentTask := 0

	completedTaskCount := 0
	for {
		shouldContinue := false

		select {
		case currentTask = <-tasks:
		case _ = <-doneTasks:
			completedTaskCount++
			shouldContinue = true
		}
		if completedTaskCount >= ntasks {
			break
		}
		if shouldContinue {
			continue
		}

		select {
		case worker = <-mr.registerChannel:
			mr.workers = append(mr.workers, worker)
		case worker = <-freeWorkers:
		}
		go dispatchWorker(worker,
			currentTask,
			mr.jobName,
			mr.files[currentTask],
			phase,
			nios,
			tasks,
			doneTasks,
			freeWorkers)
	}

	fmt.Printf("Schedule: %v phase done\n", phase)
}

func dispatchWorker(
	workerName string,
	task int,
	jobName string,
	fileName string,
	phase jobPhase,
	numOtherPhase int,
	tasksChan chan int,
	doneTasksChan chan int,
	freeWorkersChan chan string) {

	taskArgs := DoTaskArgs{
		JobName:       jobName,
		File:          fileName,
		Phase:         phase,
		TaskNumber:    task,
		NumOtherPhase: numOtherPhase}

	success := call(workerName, "Worker.DoTask", &taskArgs, new(struct{}))
	freeWorkersChan <- workerName
	if !success {
		tasksChan <- task
		return
	}

	doneTasksChan <- task

}
