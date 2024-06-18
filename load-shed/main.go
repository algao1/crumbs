package main

import (
	"container/heap"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/eapache/queue/v2"
)

// TODO:
// - Auto-Tuner
//		- https://github.com/DataDog/sketches-go
// - Priority + Cohorts
// - Graphing

const (
	// Values taken directly from Uber's blog.
	CALIBRATION_PERIOD = 500 * time.Millisecond
	REPORT_PERIOD      = 1 * time.Second
	LOOKBACK_LENGTH    = int(60 * time.Second / CALIBRATION_PERIOD)
	KP                 = 0.1
	KI                 = 1.4
	// Temporary constants.
	OPTIMAL_CONCURRENCY = 20
)

func main() {
	pid := NewPIDController()
	prod := NewProducer(500)
	prod.Run()

	ls := &LoadShedder{
		inCh:  prod.Out(),
		pid:   pid,
		sched: NewScheduler(50),
		pq:    make(MessageQueue, 0),
	}
	ls.Run()

	time.Sleep(5 * time.Second)
	prod.SetRPS(3000)
	time.Sleep(60 * time.Second)
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
			p.outCh <- Message{
				CreatedAt: time.Now(),
			}
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
	sched       *Scheduler
	pq          MessageQueue
	rejectRatio float64

	// stats
	in            uint
	out           uint
	reportIn      uint
	reportOut     uint
	reportReject  uint
	reportTimeout uint
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
			ls.reportIn++
		} else {
			ls.reportReject++
		}
		ls.mu.Unlock()
	}
}

func (ls *LoadShedder) route() {
	for range time.Tick(10 * time.Millisecond) {
		ls.mu.Lock()
		for ls.pq.Len() > 0 {
			if ls.pq[0].CreatedAt.Add(1 * time.Second).Before(time.Now()) {
				heap.Pop(&ls.pq)
				ls.reportTimeout++
				continue
			}

			if !ls.sched.CanHandle() {
				break
			}

			msg := heap.Pop(&ls.pq).(*Message)
			ls.out++
			ls.reportOut++
			ls.sched.Handle(msg)
		}
		ls.mu.Unlock()
	}
}

func (ls *LoadShedder) calibrate() {
	for range time.Tick(CALIBRATION_PERIOD) {
		ls.mu.Lock()
		inflight, inflightLimit := ls.sched.Params()
		ls.rejectRatio = ls.pid.RejectRatio(int(ls.in), int(ls.out), inflight, inflightLimit)
		ls.in = 0
		ls.out = 0
		ls.mu.Unlock()
	}
}

func (ls *LoadShedder) report() {
	for range time.Tick(REPORT_PERIOD) {
		ls.mu.Lock()
		inflight, inflightLimit := ls.sched.Params()
		fmt.Printf(
			"queue: %d, inflight: (%d/%d), in: %d, out: %d, rejected: %d, timeout: %d, ratio: %.3f\n",
			ls.pq.Len(), inflight, inflightLimit, ls.reportIn, ls.reportOut, ls.reportReject, ls.reportTimeout, ls.rejectRatio,
		)
		ls.reportIn = 0
		ls.reportOut = 0
		ls.reportReject = 0
		ls.reportTimeout = 0
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

func (p *PIDController) RejectRatio(in, out, inflight, inflightLimit int) float64 {
	denom := float64(out)
	if denom == 0 {
		denom = float64(inflightLimit)
	}

	freeInflight := float64(inflightLimit - inflight)
	errVal := (float64(in) - float64(out) - freeInflight) / denom

	p.history.Add(errVal)
	if p.history.Length() > LOOKBACK_LENGTH {
		p.history.Remove()
	}

	pTerm := KP * errVal
	iTerm := 0.0
	for i := range p.history.Length() {
		iTerm += KI * p.history.Get(i)
	}

	// Asymptotically bound by 1, so that we are always letting
	// some requests through.
	return 1 - math.Exp(-pTerm-iTerm)
}

type Scheduler struct {
	mu            sync.Mutex
	inflight      int
	inflightLimit int
}

func NewScheduler(limit int) *Scheduler {
	return &Scheduler{inflightLimit: limit}
}

func (s *Scheduler) Params() (int, int) {
	return s.inflight, s.inflightLimit
}

func (s *Scheduler) CanHandle() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inflight < s.inflightLimit
}

func (s *Scheduler) Handle(msg *Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inflight++

	go func(inflight int) {
		baseLatency := time.Duration(2*inflight) * time.Millisecond
		incrLatency := time.Duration(math.Exp(float64(inflight)/OPTIMAL_CONCURRENCY)) * time.Millisecond
		time.Sleep(baseLatency + incrLatency)

		s.mu.Lock()
		s.inflight--
		s.mu.Unlock()
	}(s.inflight)
}
