package main

import (
	"crumbs/dst"
	"fmt"
	"log/slog"
)

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

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	for i := range 16 {
		sim := dst.NewSimulator(int64(i))
		PrintHelloWorld(sim)
		sim.Run()
	}
}
