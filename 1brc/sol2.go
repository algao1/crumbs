package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"sort"

	"golang.org/x/exp/maps"
)

func process2(r io.Reader, ch chan map[string]*stat) {
	scanner := bufio.NewScanner(r)
	records := make(map[string]*stat)

	for scanner.Scan() {
		city, temp := parseTempFloat(scanner.Text())

		r, ok := records[city]
		if !ok {
			records[city] = &stat{
				count: 1,
				sumT:  temp,
				minT:  temp,
				maxT:  temp,
			}
			continue
		}

		r.count++
		r.sumT += temp
		r.minT = min(r.minT, temp)
		r.maxT = max(r.maxT, temp)
		records[city] = r
	}

	ch <- records
}

func sol2(filePath string, numWorkers int) {
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	fStat, err := file.Stat()
	if err != nil {
		log.Fatalf("unable to stat file: %v", err)
	}

	step := fStat.Size() / int64(numWorkers)
	offsets := make([]int64, numWorkers)

	for i := 1; i < numWorkers; i++ {
		pos := int64(i) * step
		file.Seek(pos, 0)

		b := make([]byte, 1)
		for b[0] != '\n' {
			file.Read(b)
			pos += 1
		}
		offsets[i] = pos
	}

	rx := make(chan map[string]*stat)

	for i := range numWorkers {
		start := offsets[i]
		end := fStat.Size()
		if i < numWorkers-1 {
			end = offsets[i+1]
		}

		go func(start, end int64) {
			r, _ := os.Open(filePath)
			defer r.Close()

			sr := io.NewSectionReader(r, start, end-start)
			process2(sr, rx)
		}(start, end)
	}

	aggrStats := make(map[string]*stat)
	for range numWorkers {
		records := <-rx
		for city, stat := range records {
			as, ok := aggrStats[city]
			if !ok {
				aggrStats[city] = stat
				continue
			}

			as.count += stat.count
			as.sumT += stat.sumT
			as.minT = min(as.minT, stat.minT)
			as.maxT = max(as.maxT, stat.maxT)
			aggrStats[city] = as
		}
	}

	cities := maps.Keys(aggrStats)
	sort.Slice(cities, func(i, j int) bool { return cities[i] < cities[j] })

	fmt.Print("{")
	for i, city := range cities {
		if i > 0 {
			fmt.Print(",")
		}
		stat := aggrStats[city]
		fmt.Printf("%s=%.1f/%.1f/%.1f", city, stat.minT, stat.sumT/float64(stat.count), stat.maxT)
	}
	fmt.Print("}\n")
}
