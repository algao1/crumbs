package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/exp/maps"
)

type stat struct {
	sumT  float64
	minT  float64
	maxT  float64
	count int
}

func parseTempFloat(line string) (string, float64) {
	split := strings.Split(line, ";")
	temp, err := strconv.ParseFloat(split[1], 64)
	if err != nil {
		log.Fatalf("unable to parse float: %v", err)
	}
	return split[0], temp
}

func sol1(filePath string) {
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	tempStats := make(map[string]*stat)
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		city, temp := parseTempFloat(scanner.Text())
		ts, ok := tempStats[city]

		if !ok {
			tempStats[city] = &stat{
				count: 1,
				sumT:  temp,
				minT:  temp,
				maxT:  temp,
			}
			continue
		}

		ts.sumT += temp
		ts.minT = min(ts.minT, temp)
		ts.maxT = max(ts.maxT, temp)
		ts.count += 1
		tempStats[city] = ts
	}

	cities := maps.Keys(tempStats)
	sort.Slice(cities, func(i, j int) bool { return cities[i] < cities[j] })

	fmt.Print("{")
	for i, city := range cities {
		if i > 0 {
			fmt.Print(",")
		}
		stat := tempStats[city]
		fmt.Printf("%s=%.1f/%.1f/%.1f", city, stat.minT, stat.sumT/float64(stat.count), stat.maxT)
	}
	fmt.Print("}\n")
}
