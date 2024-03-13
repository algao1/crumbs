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

// Inspired by https://benhoyt.com/writings/go-1brc/
func parseTempIntOptimized(b []byte) (string, int32) {
	bLen := len(b)
	temp := int32(b[bLen-3]-'0')*10 + int32(b[bLen-1]-'0')

	cutIdx := bLen - 4

	for b[cutIdx] != ';' {
		if b[cutIdx] == '-' {
			temp *= -1
		} else {
			temp += int32(b[cutIdx]-'0') * 100
		}
		cutIdx--
	}

	return string(b[:cutIdx]), temp
}

func process4(r io.Reader, ch chan map[string]*intStat) {
	scanner := bufio.NewScanner(r)
	records := make(map[string]*intStat)

	for scanner.Scan() {
		city, temp := parseTempIntOptimized(scanner.Bytes())

		r, ok := records[city]
		if !ok {
			records[city] = &intStat{
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

func sol4(filePath string, numWorkers int) {
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

	rx := make(chan map[string]*intStat)

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
			process4(sr, rx)
		}(start, end)
	}

	aggrStats := make(map[string]*intStat)
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
		fmt.Printf(
			"%s=%.1f/%.1f/%.1f",
			city,
			float64(stat.minT)/10,
			float64(stat.sumT)/float64(stat.count)/10,
			float64(stat.maxT)/10,
		)
	}
	fmt.Print("}\n")
}
