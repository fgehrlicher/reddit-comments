package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/fgehrlicher/reddit-comments/pkg/convert"
	"github.com/pkg/profile"
)

func main() {
	p := profile.Start(profile.CPUProfile, profile.ProfilePath("."), profile.NoShutdownHook)

	var (
		fileIn            = "data/RC_2019-10"
		fileOut           = "result.csv"
		chunkSize   int64 = 1024 * 1024 * 100 // 100 MiB
		workerCount       = 10
	)

	out, err := os.Create(fileOut)
	if err != nil {
		panic(err)
	}

	start := time.Now()

	chunks, err := convert.SplitFileInChunks(chunkSize, fileIn, out)
	queue := convert.NewQueue(chunks, workerCount, chunkSize)
	results := queue.Work()

	var (
		totalLines   int64
		failedChunks int
	)

	for _, result := range results {
		totalLines += int64(result.Chunk.LinesProcessed)
		if result.Err != nil {
			failedChunks++
		}
	}

	fmt.Println(strings.Repeat("-", 20))
	fmt.Printf(
		"took %v\nparsed %d lines\n%d chunks faile:\n",
		time.Since(start),
		totalLines,
		failedChunks,
	)

	p.Stop()
}
