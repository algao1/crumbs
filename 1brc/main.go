package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
)

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
	case 3:
		sol3(filePath, numWorkers)
	case 4:
		sol4(filePath, numWorkers)
	}
}
