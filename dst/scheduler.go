package dst

import (
	"container/list"
	"fmt"
	"slices"

	"golang.org/x/exp/maps"
)

// TaskScheduler is a LIRO (last-in-random-out) queue,
// which is used to randomly execute events.
type TaskScheduler struct {
	Tasks     *list.List
	Generator *Generator
	executed  map[string]int
}

func NewTaskScheduler(gen *Generator) *TaskScheduler {
	return &TaskScheduler{
		Tasks:     list.New(),
		Generator: gen,
		executed:  make(map[string]int),
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
	s.executed[task.Name]++

	if task.Callback() {
		s.Tasks.PushBack(task)
	}
}

func (s *TaskScheduler) Stats() {
	// We first sort the keys, cause otherwise map access is nondeterministic.
	keys := maps.Keys(s.executed)
	slices.Sort(keys)
	fmt.Println("============== DEBUG STATS ==============")
	for _, k := range keys {
		fmt.Printf("func %s executed %d times\n", k, s.executed[k])
	}
	fmt.Println("============== DEBUG STATS ==============")
}
