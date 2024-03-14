package dst

import "time"

type Task struct {
	Name     string
	Callback func() bool
}

type Event struct {
	T    time.Time
	Task Task
}

type EventPriority []*Event

func (pq EventPriority) Len() int {
	return len(pq)
}

func (pq EventPriority) Less(i, j int) bool {
	return pq[i].T.Before(pq[j].T)
}

func (pq EventPriority) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *EventPriority) Push(x any) {
	event := x.(*Event)
	*pq = append(*pq, event)
}

func (pq *EventPriority) Pop() any {
	old := *pq
	n := len(old)
	event := old[n-1]
	old[n-1] = nil
	*pq = old[:n-1]
	return event
}
