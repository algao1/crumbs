package dst

import (
	"container/heap"
	"time"
)

// Timer is the simulated clock for the simulation.
type Timer struct {
	CurTime   time.Time
	Scheduler *TaskScheduler
	Events    EventPriority
}

func NewTimer(gen *Generator, sched *TaskScheduler) *Timer {
	return &Timer{
		CurTime:   time.Now(),
		Scheduler: sched,
		Events:    make(EventPriority, 0),
	}
}

func (t *Timer) AddEvent(f func() bool, name string) {
	t.AddEventWithDelay(f, name, 0)
}

func (t *Timer) AddEventWithDelay(f func() bool, name string, delay time.Duration) {
	heap.Push(&t.Events, &Event{
		T: t.CurTime.Add(delay),
		Task: Task{
			Name:     name,
			Callback: f,
		},
	})
}

func (t *Timer) Execute() {
	if len(t.Events) == 0 {
		return
	}
	e := heap.Pop(&t.Events).(*Event)
	t.Scheduler.Schedule(e.Task)
	t.CurTime = e.T
}
