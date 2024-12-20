package main

import (
	"crumbs/dst"
	"fmt"
	"log/slog"
)

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	sim := dst.NewSimulator(42)
	// Equivalent to spawning 16 threads/goroutines
	// and printing "Hello World" on each.
	counter := 0
	for range 16 {
		// PrintHelloWorld(sim)
		PrintCounter(sim, &counter)
	}
	sim.Run()
	sim.Scheduler.Stats()
}

func PrintHelloWorld(sim *dst.Simulator) {
	sim.Spawn(func(yield func()) {
		yield()
		fmt.Println("Hello")
		yield()
		fmt.Println("World")
	})
}

func PrintCounter(sim *dst.Simulator, counter *int) {
	sim.Spawn(func(yield func()) {
		sim.Lock("counter_lock", yield)
		yield()
		*counter++
		yield()
		fmt.Println(*counter)
		sim.Unlock("counter_lock")
	})
}
