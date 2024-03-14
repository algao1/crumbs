package main

import (
	"crumbs/coro"
	"fmt"
	"runtime"
	"runtime/debug"
)

func main() {
	// Keep generating numbers as long as we're resuming with true.
	// This is the next(true) and next(false) part.
	// We tack on more filter coroutines that is the prime, and the
	// coroutine yield function left of it.

	// The counter's first yield goes to main, and subsequent yields go to the
	// 2-filter. Similarly, each p-filter yields its first output (the next prime)
	// to main while its subsequent yields go to the enxt filter for that next
	// prime.
	primes(10)
	debug.SetTraceback("all")
	for i := 0; i < 10; i++ {
		runtime.Gosched()
	}
	fmt.Println(runtime.NumGoroutine(), "goroutines")
}

func primes(n int) {
	next, cancel := counter()
	defer cancel()
	for range n {
		// If the corotuine panics, then it comes back in the next()
		// call. And the deferred cancel() calls clean up everything.
		//
		// If the coroutine was resumed in `filter`, then the panic
		// will propogate up to the waiting `filter` coroutine until
		// it gets to `prime`.
		p, _ := next(true)
		fmt.Println(p)
		next, cancel = filter(p, next)
		// Don't forget, this defer doesn't end up running
		// until after primes() is complete...
		defer cancel()
	}
}

// The code yields a value by passing it to yield and then receives
// back a boolean saying whether to continue generating more numbers.
// When told to stop, either because `more` was false on entry or because
// a `yield` call returned false, the loop ends.
//
// New turns this loop into a function that is the inverse of yield:
// a `func(bool) int` that can be called with true to obtain the next value,
// or shutdown with false.
func counter() (func(bool) (int, bool), func()) {
	return coro.New(func(more bool, yield func(int) bool) int {
		for i := 2; more; i++ {
			more = yield(i)
		}
		return 0
	})
}

func filter(p int, next func(bool) (int, bool)) (func(bool) (int, bool), func()) {
	return coro.New(func(more bool, yield func(int) bool) int {
		for more {
			n, _ := next(true)
			if n%p != 0 {
				more = yield(n)
			}
		}
		n, _ := next(false)
		return n
	})
}
