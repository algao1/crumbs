package main

import (
	"crumbs/dst"
	"fmt"
	"log/slog"
)

// This was made when I was going through my deterministic
// state testing "phase". I wanted to try to implement DST
// inside Go, but because of how the runtime works, it would
// require intercepting syscalls much like Reverie.

// Instead, I decided to write my own simulated environment
// where I can make certain things deterministic, such as
// goroutines yielding. This is done using coroutines, which
// aren't implemented natively in Go, yet.

// The basic idea is pretty simple, we have a simulator which
// consists of a (random number) generator, a timer that simulates
// time, and a scheduler that schedules events to be processed.
// Everything is processed in a single thread, which is the simulator.

// The scheduler is just a LIRO (last-in-random-out) queue, and
// manually yielding inserts the event back into the queue.

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	sim := dst.NewSimulator(42)
	for range 16 {
		PrintHelloWorld(sim)
	}
	sim.Run()
}

func PrintHelloWorld(sim *dst.Simulator) {
	sim.Spawn(func(yield func()) {
		PrintWorld(sim)
		yield()
		fmt.Println("Hello")
	})
}

func PrintWorld(sim *dst.Simulator) {
	sim.Spawn(func(yield func()) {
		fmt.Println("World")
		yield()
	})
}
