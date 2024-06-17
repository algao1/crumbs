package main

import (
	"container/heap"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/eapache/queue/v2"
)

// TODO:
// - Scheduler + Consumers
// - Auto-Tuner
// - Priority + Cohorts
// - Graphing

func main() {
	pid := NewPIDController()
	prod := NewProducer(50)
	prod.Run()

	ls := &LoadShedder{
		inCh: prod.Out(),
		pid:  pid,
		pq:   make(MessageQueue, 0),
	}
	ls.Run()

	time.Sleep(3 * time.Second)
	// prod.SetRPS(120)
	// time.Sleep(10 * time.Second)
	prod.SetRPS(300)
	time.Sleep(30 * time.Second)
}

type Producer struct {
	mu    sync.Mutex
	rps   float64
	outCh chan Message
}

func NewProducer(rps float64) *Producer {
	return &Producer{
		rps:   rps,
		outCh: make(chan Message),
	}
}

func (p *Producer) Out() chan Message {
	return p.outCh
}

func (p *Producer) Run() {
	go func() {
		for {
			// TODO: Generate messages with different priority here.
			p.outCh <- Message{}
			p.mu.Lock()
			delay := time.Second / time.Duration(p.rps)
			time.Sleep(delay)
			p.mu.Unlock()
		}
	}()
}

func (p *Producer) SetRPS(rps float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rps = rps
}

type LoadShedder struct {
	mu   sync.Mutex
	inCh chan Message
	// outCh     chan Message
	pid         *PIDController
	pq          MessageQueue
	rejectRatio float64

	// stats
	in       uint
	out      uint
	rejected uint
}

func (ls *LoadShedder) Run() {
	go ls.ingest()
	go ls.route()
	go ls.calibrate()
	go ls.report()
}

func (ls *LoadShedder) ingest() {
	for m := range ls.inCh {
		ls.mu.Lock()
		if rand.Float64() > ls.rejectRatio {
			heap.Push(&ls.pq, &m)
			ls.in++
		} else {
			ls.rejected++
		}
		ls.mu.Unlock()
	}
}

func (ls *LoadShedder) route() {
	for range time.Tick(10 * time.Millisecond) {
		ls.mu.Lock()
		if ls.pq.Len() > 0 {
			heap.Pop(&ls.pq)
			ls.out++
		}
		ls.mu.Unlock()
	}
}

func (ls *LoadShedder) calibrate() {
	for range time.Tick(500 * time.Millisecond) {
		ls.mu.Lock()
		ls.rejectRatio = ls.pid.RejectRatio(int(ls.in), int(ls.out), min(10, len(ls.pq)), 10)
		ls.mu.Unlock()
	}
}

func (ls *LoadShedder) report() {
	for range time.Tick(1 * time.Second) {
		ls.mu.Lock()
		fmt.Printf(
			"queue: %d, in: %d, out: %d, rejected: %d, ratio: %.3f\n",
			ls.pq.Len(), ls.in, ls.out, ls.rejected, ls.rejectRatio,
		)
		ls.in = 0
		ls.out = 0
		ls.rejected = 0
		ls.mu.Unlock()
	}
}

type PIDController struct {
	history *queue.Queue[float64]
}

func NewPIDController() *PIDController {
	return &PIDController{
		history: queue.New[float64](),
	}
}

func (p *PIDController) RejectRatio(in, out, inFlight, inFlightLimit int) float64 {
	denom := float64(out)
	if denom == 0 {
		denom = float64(inFlightLimit)
	}

	const (
		Kp = 0.1
		Ki = 0.25
	)

	freeInFlight := float64(inFlightLimit - inFlight)
	errVal := (float64(in) - float64(out) - freeInFlight) / denom
	pTerm := Kp * errVal

	p.history.Add(errVal)
	if p.history.Length() > 30 {
		p.history.Remove()
	}

	iTerm := 0.0
	for i := range p.history.Length() {
		iTerm += Ki * p.history.Get(i)
	}

	return pTerm + iTerm
}
