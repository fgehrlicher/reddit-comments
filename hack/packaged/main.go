package main

import (
	"github.com/fgehrlicher/reddit-comments/pkg/convert"
	"github.com/pkg/profile"
	"os"
)

func main() {
	p := profile.Start(profile.TraceProfile, profile.ProfilePath("."), profile.NoShutdownHook)

	var (
		fileIn            = "data/test_10gb"
		fileOut           = "result.csv"
		chunkSize   int64 = 1024 * 10 // 1 KiB
		workerCount       = 10
	)

	out, err := os.Create(fileOut)
	if err != nil {
		panic(err)
	}

	chunks, err := convert.SplitFileInChunks(chunkSize, fileIn, out)

	queue := convert.Queue{
		Workers: workerCount,
		Chunks:  chunks,
	}

	queue.Work()

	p.Stop()
}
