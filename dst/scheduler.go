package dst

import (
	"container/list"
)

// TaskScheduler is a LIRO (last-in-random-out) queue,
// which is used to randomly execute events.
type TaskScheduler struct {
	Tasks     *list.List
	Generator *Generator
}

func NewTaskScheduler(gen *Generator) *TaskScheduler {
	return &TaskScheduler{
		Tasks:     list.New(),
		Generator: gen,
	}
}

func (s *TaskScheduler) Schedule(task Task) {
	s.Tasks.PushBack(task)
}

func (s *TaskScheduler) Execute() {
	if s.Tasks.Len() == 0 {
		return
	}
	shifts := s.Generator.Rand() % (s.Tasks.Len())
	cur := s.Tasks.Front()
	for range shifts {
		cur = cur.Next()
	}

	task := s.Tasks.Remove(cur).(Task)
	if task.Callback() {
		s.Tasks.PushBack(task)
	}
}
