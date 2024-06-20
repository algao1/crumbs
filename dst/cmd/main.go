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
	for range 16 {
		PrintHelloWorld(sim)
	}
	sim.Run()
}

func PrintHelloWorld(sim *dst.Simulator) {
	sim.Spawn(func(yield func()) {
		yield()
		fmt.Println("Hello")
		yield()
		fmt.Println("World")
	})
}
