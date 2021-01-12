package main

import (
	"github.com/fgehrlicher/reddit-comments/pkg/convert"
	"github.com/pkg/profile"
	"os"
)

func main() {
	p := profile.Start(profile.CPUProfile, profile.ProfilePath("."), profile.NoShutdownHook)

	var (
		fileIn            = "data/test_1mb"
		fileOut           = "result.csv"
		chunkSize   int64 = 1024 *10 // 100 MiB
		workerCount       = 6
	)

	out, err := os.Create(fileOut)
	if err != nil {
		panic(err)
	}

	chunks, err := convert.SplitFileInChunks(chunkSize, fileIn, out)

	queue := convert.NewQueue(chunks, workerCount, chunkSize)
	queue.Work()

	p.Stop()
}
