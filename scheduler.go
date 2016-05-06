package scheduler

import (
	"errors"
	"sync"

	"golang.org/x/net/context"
)

// A Task is work to be scheduled by the Scheduler. It is best to always set
// a deadline/timeout in the context for tasks to ensure that submitted Tasks
// cannot cause a Scheduler deadlock.
type Task interface {
	// Context() returns the execution context for this task. This can be
	// used to cancel a task or provide a timeout--as well as send data
	// into a task to be used during execution.
	Context() context.Context

	// Execute() must return a non-nil error upon success. If an error is
	// returned alongside a result, there is no guarantee that the the
	// result will be read from the encapsulating Job.
	Execute() (interface{}, error)
}

// A Job is a unit of work to be done by the scheduler. It encapsulates a task
// throughout its lifetime in the scheduler.
type Job struct {
	Task       Task
	errChan    chan error
	resultChan chan interface{}
}

// Result returns the interface{} returned by a Task's Execute() method or an
// error if there was a problem executing a task or if the task returned an
// error itself. Context errors will also be returned here.
func (j *Job) Result() (interface{}, error) {
	select {
	case r := <-j.resultChan:
		return r, nil
	case err := <-j.errChan:
		return nil, err
	}
}

// Scheduler manages the parallel execution of a number of jobs.
type Scheduler struct {
	// MaxQueueDepth specifies the number of outstanding Jobs a scheduler
	// will allow before it will stop accepting jobs.
	MaxQueueDepth uint

	jobqueue chan *Job
	workers  chan int
	mutex    sync.Mutex
}

// NewScheduler initializes a Scheduler, and it is the only correct way to
// initialize one. It takes as its arguments the maximum number of concurrent
// tasks that the Scheduler can be running.
func NewScheduler(maxJobs uint) *Scheduler {
	s := &Scheduler{
		MaxQueueDepth: maxJobs,
		workers:       make(chan int, maxJobs),
		jobqueue:      make(chan *Job, maxJobs),
	}
	for i := 0; i < int(maxJobs); i++ {
		s.workers <- i
	}

	s.start()
	return s
}

// Submit accepts a task and returns either the corresponding Job for that task
// or an error (if the task cannot be submitted at this time). A Job is a
// contract for future execution--unless the context for the task is cancelled
// (either manually or via timeout/deadline).
func (s *Scheduler) Submit(t Task) (*Job, error) {
	s.mutex.Lock()
	if uint(len(s.jobqueue)) >= s.MaxQueueDepth {
		return nil, errors.New("Task would exceed MaxQueueDepth.")
	}

	j := &Job{
		Task:       t,
		errChan:    make(chan error, 1),
		resultChan: make(chan interface{}, 1),
	}
	s.jobqueue <- j
	s.mutex.Unlock()

	return j, nil
}

// QueueDepth returns the current number of jobs waiting to be scheduled.
func (s *Scheduler) QueueDepth() int {
	return len(s.jobqueue)
}

func (s *Scheduler) start() {
	go func() {
		for job := range s.jobqueue {
			t := job.Task
			select {
			case <-t.Context().Done():
				job.errChan <- t.Context().Err()
			case id := <-s.workers:
				go func(j *Job, i int) {
					res, err := j.Task.Execute()
					if err != nil {
						j.errChan <- err
						return
					}

					j.resultChan <- res
					s.workers <- i
				}(job, id)
			}
		}

	}()
}
