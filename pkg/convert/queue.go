package convert

import (
	"fmt"
	"strconv"
	"sync"
	"time"
)

type Queue struct {
	Workers int
	Chunks  []Chunk
}

func (queue *Queue) Work() {
	start := time.Now()

	chunkHead := 0
	resultChan := make(chan ProcessResult, queue.Workers)
	var wg sync.WaitGroup

	wg.Add(len(queue.Chunks))
	for i := 0; i < queue.Workers; i++ {
		if chunkHead >= len(queue.Chunks) {
			break
		}

		go queue.Chunks[chunkHead].Process(resultChan, &wg)
		chunkHead++
	}

	failedChunks := make([]ProcessResult, 0)

	quit := make(chan int)
	go func() {
		chunksProcessed := 0

		for {
			select {
			case result := <-resultChan:
				chunksProcessed++

				if result.err != nil {
					fmt.Printf(
						"[%*d/%d] error in chunk :%s\n",
						len(strconv.Itoa(len(queue.Chunks))),
						result.chunk.id,
						len(queue.Chunks),
						result.err,
					)
					failedChunks = append(failedChunks, result)

				} else {
					fmt.Printf(
						"[%*d/%d] done %.2f %%\n",
						len(strconv.Itoa(len(queue.Chunks))),
						result.chunk.id,
						len(queue.Chunks),
						float32(chunksProcessed)/float32(len(queue.Chunks))*100,
					)
				}

				if chunkHead < len(queue.Chunks) {
					go queue.Chunks[chunkHead].Process(resultChan, &wg)
					chunkHead++
				}
			case <-quit:
				return
			}
		}
	}()

	wg.Wait()
	quit <- 0
	fmt.Printf("took %v\n", time.Since(start))
	fmt.Printf("%d Chunks failed:\n", len(failedChunks))
	for _, failedChunk := range failedChunks {
		fmt.Printf("%d Chunks failed: %s\n", failedChunk.chunk.id, failedChunk.err)
	}
}
