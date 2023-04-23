package main

import (
	"context"
	"crumbs/p2p-rpc/node"
	"log"
	"math/rand"
	"os"
	"time"
)

func main() {
	args := os.Args[1:]

	if len(args) < 3 {
		log.Fatal("arguments required: <name> <listening address> <consul address>")
	}

	name := args[0]
	addr := args[1]
	sdAddress := args[2]

	node := node.New(name, addr, sdAddress)
	ticker := time.NewTicker(time.Duration(rand.Float64()*5) * time.Second)
	for {
		<-ticker.C
		node.PingAll(context.Background())
	}
}
