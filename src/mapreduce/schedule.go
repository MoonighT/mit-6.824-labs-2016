package mapreduce

import (
	"fmt"
	"sync"
)

// schedule starts and waits for all jobs in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var njobs int
	var nios int // number of inputs (for reduce) or outputs (for map)
	var otherphase int
	switch phase {
	case mapPhase:
		njobs = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		njobs = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v jobs (%d I/Os)\n", njobs, phase, nios)

	// All njobs jobs have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple jobs.
	var wg sync.WaitGroup
	wg.Add(njobs)

	type Job struct {
		file  string
		index int
	}
	for i, _ := range mr.files {
		go func(index int) {
			w := <-mr.registerChannel
			arg := &DoJobArgs{
				JobName:       mr.jobName,
				File:          mr.files[index],
				Phase:         phase,
				JobNumber:     index,
				NumOtherPhase: nios,
			}
			ok := call(w, "Worker.DoJob", arg, new(struct{}))
			if !ok {
				debug("call not ok")
			} else {
				mr.registerChannel <- w
				wg.Done()
				break
			}
		}(i)
	}
	wg.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}
