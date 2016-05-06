package scheduler

import (
	"testing"
	"time"

	"golang.org/x/net/context"
)

type testTask struct {
	id       int
	ctx      context.Context
	result   int
	executed bool
}

func (j *testTask) Context() context.Context {
	return j.ctx
}

func (j *testTask) Execute() (interface{}, error) {
	j.executed = true
	time.Sleep(20 * time.Millisecond)
	return 1, nil
}

func TestMaxConcurrency(t *testing.T) {
	// I realize this is not the best test in the world. It gets the job
	// done though. -greg

	s := NewScheduler(2)
	s.MaxQueueDepth = 3
	var ctx context.Context

	ctx, _ = context.WithTimeout(context.Background(), 5*time.Millisecond)
	t1 := &testTask{1, ctx, 0, false}
	j1, err := s.Submit(t1)
	if err != nil || j1 == nil {
		t.Error("Error scheduling t1")
	}

	ctx, _ = context.WithTimeout(context.Background(), 5*time.Millisecond)
	t2 := &testTask{2, ctx, 0, false}
	j2, err := s.Submit(t2)
	if err != nil || j2 == nil {
		t.Error("Error scheduling t2")
	}

	ctx, _ = context.WithTimeout(context.Background(), 5*time.Millisecond)
	t3 := &testTask{3, ctx, 0, false}
	j3, err := s.Submit(t3)
	if err != nil || j3 == nil {
		t.Error("Error scheduling t3: ", err)
	}

	time.Sleep(100 * time.Millisecond)
	if res, err := j1.Result(); res == 0 || err != nil || !t1.executed {
		t.Error("j1 failed.")
	}

	if res, err := j2.Result(); res == 0 || err != nil || !t2.executed {
		t.Error("j2 failed.")
	}

	if res, err := j3.Result(); err == nil || res != nil {
		t.Error("Expected j3 to fail with error and return no result.")
	}

	if t3.executed {
		t.Error("Expected t3 not to be executed.")
	}
}
