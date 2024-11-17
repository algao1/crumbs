package main

import (
	"bufio"
	"crumbs/badsql"
	"fmt"
	"os"
)

const (
	PromptStr = ">> "
)

func main() {
	executor := badsql.NewExecutor()
	scanner := bufio.NewScanner(os.Stdin)

	if len(os.Args) > 1 {
		file, err := os.Open(os.Args[1])
		if err != nil {
			panic(err)
		}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			fmt.Println("executing line:", scanner.Text())
			err := executor.HandleStatement(scanner.Text())
			if err != nil {
				fmt.Println("error:", err)
			}
		}
		return
	}

	fmt.Print(PromptStr)
	for scanner.Scan() {
		err := executor.HandleStatement(scanner.Text())
		if err != nil {
			fmt.Println("error:", err)
		}
		fmt.Print(PromptStr)
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("error:", err)
	}
}
