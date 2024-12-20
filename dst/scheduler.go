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

	// Internals.
	curFunc   string
	heldLocks map[string]string
	executed  map[string]int
}

func NewTaskScheduler(gen *Generator) *TaskScheduler {
	return &TaskScheduler{
		Tasks:     list.New(),
		Generator: gen,
		heldLocks: make(map[string]string),
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
	s.curFunc = task.Name
	s.executed[task.Name]++

	if task.Callback() {
		s.Tasks.PushBack(task)
	}
}

func (s *TaskScheduler) Lock(lockID string) bool {
	if _, ok := s.heldLocks[lockID]; ok {
		return false
	}
	s.heldLocks[lockID] = s.curFunc
	return true
}

func (s *TaskScheduler) Unlock(lockID string) {
	delete(s.heldLocks, lockID)
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
