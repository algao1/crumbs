package main

import "time"

type Message struct {
	Payload   any
	Priority  int
	Cohort    int
	CreatedAt time.Time
}

type MessageQueue []*Message

func (pq MessageQueue) Len() int {
	return len(pq)
}

func (pq MessageQueue) Less(i, j int) bool {
	return pq[i].Priority*NUM_COHORTS+pq[i].Cohort < pq[j].Priority*NUM_COHORTS+pq[j].Cohort
}

func (pq MessageQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *MessageQueue) Push(x any) {
	event := x.(*Message)
	*pq = append(*pq, event)
}

func (pq *MessageQueue) Pop() any {
	old := *pq
	n := len(old)
	event := old[n-1]
	old[n-1] = nil
	*pq = old[:n-1]
	return event
}
