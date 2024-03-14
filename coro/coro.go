package coro

import (
	"errors"
	"fmt"
)

// Code directly from: https://research.swtch.com/coro

var ErrCanceled = errors.New("coroutine canceled")

type msg[T any] struct {
	panic any
	val   T
}

// This is a push iterator?
func New[In, Out any](f func(in In, yield func(Out) In) Out) (
	resume func(In) (Out, bool), cancel func(),
) {
	cin := make(chan msg[In])
	cout := make(chan msg[Out])
	running := true

	// resume and yield functions forms a pair
	// - caller gets resume
	// - calling function gets yield
	// I think the general idea is that we're switching work between the two.
	resume = func(in In) (out Out, ok bool) {
		if !running {
			return
		}
		cin <- msg[In]{val: in}
		m := <-cout
		if m.panic != nil {
			panic(m.panic)
		}
		return m.val, running
	}
	cancel = func() {
		e := fmt.Errorf("%w", ErrCanceled)
		cin <- msg[In]{panic: e}
		m := <-cout
		if m.panic != nil && m.panic != e {
			panic(m.panic)
		}
	}
	yield := func(out Out) In {
		cout <- msg[Out]{val: out}
		m := <-cin
		if m.panic != nil {
			panic(m.panic)
		}
		return m.val
	}

	// blocks on <-cin, so no opportunity for parallelism
	go func() {
		defer func() {
			if running {
				running = false
				cout <- msg[Out]{panic: recover()}
			}
		}()
		var out Out
		m := <-cin
		if m.panic == nil {
			out = f(m.val, yield)
		}
		running = false
		cout <- msg[Out]{val: out}
	}()

	return resume, cancel
}

func Pull[V any](push func(yield func(V) bool)) (pull func() (V, bool), stop func()) {
	// Start a corotuine to run the push iterator, it needs a wrapper
	// function with the right type.
	copush := func(more bool, yield func(V) bool) V {
		if more {
			push(yield)
		}
		var zero V
		return zero
	}
	resume, _ := New(copush)
	pull = func() (V, bool) {
		return resume(true)
	}
	stop = func() {
		resume(false)
	}
	return pull, stop
}
