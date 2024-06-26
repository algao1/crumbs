package dst

import (
	"crumbs/coro"
	"log/slog"
	"runtime"
)

// This was made when I was going through my deterministic
// state testing "phase". I wanted to try to implement DST
// inside Go, but because of how the runtime works, it would
// require intercepting syscalls much like Reverie.

// Instead, I decided to write my own simulated environment
// where I can make certain things deterministic, such as
// goroutines yielding. This is done using coroutines, which
// aren't implemented natively in Go, yet.

// The basic idea is pretty simple, we have a simulator which
// consists of a (random number) generator, a timer that simulates
// time, and a scheduler that schedules events to be processed.
// Everything is processed in a single thread, which is the simulator.

// The scheduler is just a LIRO (last-in-random-out) queue, and
// manually yielding inserts the event back into the queue.

type Simulator struct {
	Generator *Generator
	Timer     *Timer
	Scheduler *TaskScheduler
}

func NewSimulator(seed int64) *Simulator {
	gen := NewGenerator(seed)
	scheduler := NewTaskScheduler(gen)
	timer := NewTimer(gen, scheduler)
	s := &Simulator{
		Generator: gen,
		Timer:     timer,
		Scheduler: scheduler,
	}
	return s
}

func (s *Simulator) Run() {
	for {
		// TODO: Can probably make this more customizable.
		for range s.Generator.Rand() % 5 {
			s.Timer.Execute()
		}
		for range s.Generator.Rand() % 50 {
			s.Scheduler.Execute()
		}

		if len(s.Timer.Events) == 0 &&
			s.Scheduler.Tasks.Len() == 0 {
			return
		}
	}
}

func (s *Simulator) Spawn(f func(yield func())) {
	pc, _, _, _ := runtime.Caller(1)
	funcName := runtime.FuncForPC(pc).Name()

	resume, _ := coro.New(func(_ bool, yield func(int) bool) int {
		f(func() {
			slog.Debug("function yielded", slog.String("func", funcName))
			yield(0)
		})
		return 0
	})

	s.Timer.AddEvent(
		func() bool {
			_, ok := resume(false)
			return ok
		},
		funcName,
	)
}
