package scheduler

import (
	"testing"
	"time"

	"golang.org/x/net/context"
)

type testTask struct {
	id        int
	ctx       context.Context
	result    int
	executed  bool
	executefn func(*testTask) (interface{}, error)
}

func (t *testTask) Context() context.Context {
	return t.ctx
}

func (t *testTask) Execute() (interface{}, error) {
	return t.executefn(t)
}

func TestMaxConcurrency(t *testing.T) {
	// I realize this is not the best test in the world. It gets the job
	// done though. -greg

	executefn := func(t *testTask) (interface{}, error) {
		t.executed = true
		time.Sleep(20 * time.Millisecond)
		return 1, nil
	}

	s := NewScheduler(2)
	var ctx context.Context

	ctx, _ = context.WithTimeout(context.Background(), 5*time.Millisecond)
	t1 := &testTask{1, ctx, 0, false, executefn}
	j1, err := s.Submit(t1)
	if err != nil || j1 == nil {
		t.Error("Error scheduling t1")
	}

	ctx, _ = context.WithTimeout(context.Background(), 5*time.Millisecond)
	t2 := &testTask{2, ctx, 0, false, executefn}
	j2, err := s.Submit(t2)
	if err != nil || j2 == nil {
		t.Error("Error scheduling t2")
	}

	ctx, _ = context.WithTimeout(context.Background(), 5*time.Millisecond)
	t3 := &testTask{3, ctx, 0, false, executefn}
	j3, err := s.Submit(t3)
	if err == nil || j3 != nil {
		t.Error("Scheduling t3 should have failed.")
	}

	time.Sleep(100 * time.Millisecond)
	if res, err := j1.Result(); res == 0 || err != nil || !t1.executed {
		t.Error("j1 failed.")
	}

	if res, err := j2.Result(); res == 0 || err != nil || !t2.executed {
		t.Error("j2 failed.")
	}
}

func TestMaxQueueDepth(t *testing.T) {
	c := make(chan interface{})
	executefn := func(t *testTask) (interface{}, error) {
		t.executed = true
		<-c
		return 1, nil
	}

	s := NewScheduler(1)
	// MaxQueueDepth is 1

	t1 := &testTask{1, context.Background(), 0, false, executefn}
	j1, err := s.Submit(t1)
	if err != nil || j1 == nil {
		t.Error("Error scheduling t1")
	}

	// Eventually t1 will start executing, retry scheduling this until it is
	// accepted... to some extent.
	var retryCount int

	// Sorry nerds, but this is more readable.
	t2 := &testTask{1, context.Background(), 0, false, executefn}
	var j2 *Job
	for retryCount = 1; retryCount <= 10; retryCount++ {
		j2, err = s.Submit(t2)
		if err != nil || j2 == nil {
			time.Sleep(1 * time.Millisecond)
			continue
		} else {
			break
		}
	}
	if j2 == nil {
		t.Error("Failed to schedule t2.")
		t.FailNow()
	}

	if s.QueueDepth() != 1 {
		t.Error("Queue depth is not 1.", s.QueueDepth())
		t.Fail()
	}

	t3 := &testTask{1, context.Background(), 0, false, executefn}
	j3, err := s.Submit(t3)
	if err == nil || j3 != nil {
		t.Error("Expected not to be able to schedule t3")
	}

	// Clear the queue.
	close(c)

	c2 := make(chan interface{})
	defer close(c2)

	executefn2 := func(t *testTask) (interface{}, error) {
		t.executed = true
		<-c2
		return 1, nil
	}

	t4 := &testTask{1, context.Background(), 0, false, executefn2}
	var j4 *Job
	for retryCount = 1; retryCount <= 10; retryCount++ {
		j4, err = s.Submit(t4)
		if err != nil || j4 == nil {
			time.Sleep(1 * time.Millisecond)
			continue
		} else {
			break
		}
	}
	if j4 == nil {
		t.Error("Failed to schedule t4.")
	}
}

func TestContextCancelled(t *testing.T) {
	executefn := func(t *testTask) (interface{}, error) {
		t.executed = true
		return 1, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s := NewScheduler(1)
	t1 := &testTask{1, ctx, 0, false, executefn}
	_, err := s.Submit(t1)
	if err != nil {
		t.Error("Could not schedule task.")
	}

	if t1.executed {
		t.Error("Executed task with cancelled centext.")
	}
}
