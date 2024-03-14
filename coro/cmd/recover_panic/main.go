package main

import (
	"crumbs/coro"
	"fmt"
)

func main() {
	defer func() {
		if e := recover(); e != nil {
			fmt.Println("main panic:", e)
			panic(e)
		}
	}()
	next, _ := coro.Pull(func(yield func(string) bool) {
		yield("hello")
		panic("world")
	})
	for {
		fmt.Println(next())
	}
}
