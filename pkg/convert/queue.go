package convert

import (
	"fmt"
	"strconv"
	"sync"
	"time"
)

type Queue struct {
	workers    int
	chunkCount int
	chunkSize  int64

	tasks  chan Chunk
	result chan ProcessResult
}

func NewQueue(chunks []Chunk, workers int, chunkSize int64) *Queue {
	tasks := make(chan Chunk, len(chunks))
	for _, chunk := range chunks {
		tasks <- chunk
	}
	close(tasks)

	return &Queue{
		workers:    workers,
		tasks:      tasks,
		result:     make(chan ProcessResult, workers),
		chunkCount: len(chunks),
		chunkSize:  chunkSize,
	}
}

func (queue *Queue) Work() {
	var (
		waitGroup    sync.WaitGroup
		start        = time.Now()
		failedChunks = make([]ProcessResult, 0)
	)

	waitGroup.Add(queue.workers)
	for i := 0; i < queue.workers; i++ {
		go NewWorker(queue.tasks, queue.result, queue.chunkSize, &waitGroup).Work()
	}

	quit := make(chan int)
	go func() {
		chunksProcessed := 0

		for {
			select {
			case result := <-queue.result:
				chunksProcessed++

				if result.err != nil {
					fmt.Printf(
						"[%*d/%d] error in chunk :%s\n",
						len(strconv.Itoa(queue.chunkCount)),
						result.chunk.id,
						queue.chunkCount,
						result.err,
					)
					failedChunks = append(failedChunks, result)

				} else {
					fmt.Printf(
						"[%*d/%d] done %.2f %%\n",
						len(strconv.Itoa(queue.chunkCount)),
						result.chunk.id,
						queue.chunkCount,
						float32(chunksProcessed)/float32(queue.chunkCount)*100,
					)
				}

			case <-quit:
				return
			}
		}
	}()

	waitGroup.Wait()
	quit <- 0
	fmt.Printf("took %v\n", time.Since(start))
	fmt.Printf("%d Chunks failed:\n", len(failedChunks))
	for _, failedChunk := range failedChunks {
		fmt.Printf("chunk %d failed: %s\n", failedChunk.chunk.id, failedChunk.err)
	}
}
