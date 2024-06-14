package dst

import (
	"crumbs/coro"
	"log/slog"
	"runtime"
)

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
		for range s.Generator.Rand() % 5 {
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
