package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
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

var (
	sol        int
	numWorkers int
	filePath   string
	profile    bool
)

func init() {
	flag.IntVar(&sol, "sol", 1, "solution implementation")
	flag.IntVar(&numWorkers, "numWorkers", runtime.NumCPU(), "number of workers")
	flag.StringVar(&filePath, "filePath", "m_medium.txt", "filepath")
	flag.BoolVar(&profile, "profile", false, "profile cpu")
	flag.Parse()
}

func main() {
	if profile {
		f, err := os.Create("cpu_profile.pprof")
		if err != nil {
			fmt.Println("unable to create CPU profile: ", err)
			return
		}
		defer f.Close()

		if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Println("unable to start CPU profile: ", err)
			return
		}
		defer pprof.StopCPUProfile()
	}

	switch sol {
	case 1:
		sol1(filePath)
	case 2:
		sol2(filePath, numWorkers)
	}
}