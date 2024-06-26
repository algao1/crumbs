package main

import (
	"container/heap"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/DataDog/sketches-go/ddsketch"
	"github.com/eapache/queue/v2"
	"gonum.org/v1/gonum/stat"
)

// Basic Message Flow:
//
// Producer -> Rejector -> Priority Queue -> Scheduler -> Consumer
//			   ^^^^^^^^						 ^^^^^^^^^
//			  (PID Controller)				(Auto-Tuner)
//
// The PID controller attempts to find a good ratio of requests to
// reject/shed so that the queue is not overwhelmed.
// The auto-tuner measures latency and adjusts the maximum number
// of concurrent requests to prevent compute from being overloaded.
// The consumer simulates request latencies using an exponential
// function, this has some side effects such as making the auto-tuner
// overly aggressive.
//
// There is a small problem currently where the auto-tuner will
// aggressively decrease the inflight limit in attempt to decrease
// latency (primarily from resetThreshold), since the covariance
// is negative.
// Note: This might not be true, need to run for a longer period to verify.
//
// To combat this, we might want to more carefully consider the tradeoff
// between latency and throughput, include some measure of CPU utilization
// to ensure that it is properly utilized, or raise the minimum limit.

const (
	// Values taken directly from Uber's blog.
	// https://www.uber.com/en-CA/blog/cinnamon-using-century-old-tech-to-build-a-mean-load-shedder/
	// PID controller values.
	CALIBRATION_PERIOD = 500 * time.Millisecond
	REPORT_PERIOD      = 1 * time.Second
	LOOKBACK_LENGTH    = int(60 * time.Second / CALIBRATION_PERIOD)
	KP                 = 0.1
	KI                 = 1.4
	// Auto-tuner values.
	SAMPLE_QUANTILE          = 0.9
	MIN_INTERVAL_COUNT       = 250
	MIN_INTERVAL_TIME        = 3 * time.Second
	LIMIT_CALIBRATION_PERIOD = 5 * time.Second

	// Temporary constants.
	QUEUE_EXPIRATION_DURATION = 1 * time.Second
	LATENCY_MODIFIER          = 10 // Smaller -> larger latency.
	MIN_CONCURRENT_LIMIT      = 10
)

func main() {
	prod := NewProducer(500)
	ls := NewLoadShedder(prod, 50)

	ls.Run()
	prod.Run()

	time.Sleep(30 * time.Second)
	prod.SetRPS(3000)
	time.Sleep(90 * time.Second)
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
	mu          sync.Mutex
	inCh        chan Message
	rejectRatio float64
	pq          MessageQueue
	sched       *Scheduler
	pid         *PIDController
	tuner       *AutoTuner

	// stats
	in  uint
	out uint
	// report stats
	reportIn      uint
	reportOut     uint
	reportReject  uint
	reportTimeout uint
}

func NewLoadShedder(prod *Producer, maxConcurrency int) *LoadShedder {
	sched := NewScheduler(0)
	return &LoadShedder{
		inCh:  prod.Out(),
		pid:   NewPIDController(),
		tuner: NewAutoTuner(maxConcurrency, sched),
		sched: sched,
		pq:    make(MessageQueue, 0),
	}
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
		// TODO: Reject by priority and by cohort.
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
			if ls.pq[0].CreatedAt.Add(QUEUE_EXPIRATION_DURATION).Before(time.Now()) {
				heap.Pop(&ls.pq)
				ls.reportTimeout++
				continue
			}

			if !ls.sched.CanHandle() {
				break
			}
			msg := heap.Pop(&ls.pq).(*Message)
			ls.sched.Handle(msg, func(latency float64) {
				ls.tuner.Add(latency)
			})

			ls.out++
			ls.reportOut++
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
		reqLatency := ls.sched.GetResetLatency()
		fmt.Printf(
			"queue: %d, inflight: (%d/%d), in: %d, out: %d, rejected: %d, timeout: %d, ratio: %.3f, latency: %.3f\n",
			ls.pq.Len(), inflight, inflightLimit, ls.reportIn, ls.reportOut, ls.reportReject, ls.reportTimeout, ls.rejectRatio, reqLatency,
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

	// We don't include the D term since the paper mentions that its not
	// that useful.
	// Asymptotically bound result by 1, so that we are more likely to let
	// some requests through when value is large.
	return 1 - math.Exp(-pTerm-iTerm)
}

type LatencyInterval struct {
	maxInflight int
	sketch      *ddsketch.DDSketch
}

type AutoTuner struct {
	mu            sync.Mutex
	sched         *Scheduler
	sketch        *ddsketch.DDSketch
	intervalStart time.Time
	limit         int
	targetLatency float64
	history       *queue.Queue[LatencyInterval]
}

func NewAutoTuner(limit int, sched *Scheduler) *AutoTuner {
	sketch, _ := ddsketch.NewDefaultDDSketch(0.1)
	at := &AutoTuner{
		sched:         sched,
		sketch:        sketch,
		intervalStart: time.Now(),
		limit:         limit,
		targetLatency: 9999,
		history:       queue.New[LatencyInterval](),
	}
	sched.SetLimit(limit)
	go at.periodicallyAdjustLimit()
	return at
}

func (at *AutoTuner) Add(latency float64) {
	at.mu.Lock()
	defer at.mu.Unlock()

	at.sketch.Add(float64(latency))
	at.targetLatency = min(at.targetLatency, latency)

	if at.sketch.GetCount() > MIN_INTERVAL_COUNT &&
		time.Since(at.intervalStart) > MIN_INTERVAL_TIME {
		at.history.Add(LatencyInterval{
			maxInflight: at.sched.GetResetMaxInflight(),
			sketch:      at.sketch.Copy(),
		})

		if at.history.Length() > 50 {
			at.history.Remove()
		}
		at.resetThreshold()
		at.sketch.Clear()
		at.intervalStart = time.Now()
	}
}

func (at *AutoTuner) resetThreshold() {
	xs := make([]float64, at.history.Length())
	ys := make([]float64, at.history.Length())
	ws := make([]float64, at.history.Length())

	var err error
	for i := range at.history.Length() {
		xs[i], err = at.history.Get(i).sketch.GetValueAtQuantile(0.9)
		if err != nil {
			continue
		}
		ys[i] = float64(at.history.Get(i).maxInflight) / xs[i]
		ws[i] = 1
	}

	cov := stat.Covariance(xs, ys, ws)
	if cov < 0 {
		at.setLimit(at.limit - 1)
	}
	at.targetLatency = 9999
}

func (at *AutoTuner) periodicallyAdjustLimit() {
	for range time.Tick(LIMIT_CALIBRATION_PERIOD) {
		at.mu.Lock()
		if at.history.Length() == 0 {
			at.mu.Unlock()
			continue
		}

		// TODO: These threshold values are kinda arbitrary, the blog
		// doesn't really mention them?
		bottomThresh := -2 * math.Log10(float64(at.limit)/100)
		upperThresh := -4 * math.Log10(float64(at.limit)/100)

		q, _ := at.history.Peek().sketch.GetValueAtQuantile(SAMPLE_QUANTILE)
		v := q / at.targetLatency

		if v < bottomThresh {
			at.setLimit(at.limit + 1)
		} else if v > upperThresh {
			at.setLimit(at.limit - 1)
		}
		at.mu.Unlock()
	}
}

func (at *AutoTuner) setLimit(limit int) {
	at.limit = limit
	at.limit = max(at.limit, MIN_CONCURRENT_LIMIT)
	at.sched.SetLimit(at.limit)
}

type Scheduler struct {
	mu            sync.Mutex
	maxInflight   int
	inflight      int
	inflightLimit int
	latencies     *ddsketch.DDSketch
}

func NewScheduler(limit int) *Scheduler {
	sketch, _ := ddsketch.NewDefaultDDSketch(0.1)
	return &Scheduler{
		inflightLimit: limit,
		latencies:     sketch,
	}
}

func (s *Scheduler) SetLimit(limit int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inflightLimit = limit
}

func (s *Scheduler) GetResetMaxInflight() int {
	// TODO: This is maybe not the most elegant design.
	s.mu.Lock()
	defer s.mu.Unlock()
	ret := s.maxInflight
	s.maxInflight = 0
	return ret
}

func (s *Scheduler) Params() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inflight, s.inflightLimit
}

func (s *Scheduler) CanHandle() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inflight < s.inflightLimit
}

func (s *Scheduler) Handle(msg *Message, doneFn func(float64)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inflight++
	s.maxInflight = max(s.maxInflight, s.inflight)

	go func(inflight int) {
		baseLatency := time.Duration(100+50*rand.NormFloat64()) * time.Millisecond
		incrLatency := time.Duration(math.Exp(float64(inflight)/LATENCY_MODIFIER)) * time.Millisecond
		latency := baseLatency + incrLatency
		time.Sleep(latency)

		s.latencies.Add(latency.Seconds())
		doneFn(latency.Seconds())

		s.mu.Lock()
		s.inflight--
		s.mu.Unlock()
	}(s.inflight)
}

func (s *Scheduler) GetResetLatency() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	latency, _ := s.latencies.GetValueAtQuantile(SAMPLE_QUANTILE)
	s.latencies.Clear()
	return latency
}
