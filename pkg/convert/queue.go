package convert

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Queue struct {
	workers    int
	chunkCount int
	chunkSize  int64

	tasks  chan Chunk
	result chan ChunkResult
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
		result:     make(chan ChunkResult, workers),
		chunkCount: len(chunks),
		chunkSize:  chunkSize,
	}
}

func (queue *Queue) Work() {
	var (
		waitGroup    sync.WaitGroup
		start              = time.Now()
		failedChunks       = make([]ChunkResult, 0)
		totalLines   int64 = 0
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
				totalLines += int64(result.chunk.processedLines)

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
					percent := float32(chunksProcessed) / float32(queue.chunkCount) * 100
					percentPadding := ""
					if percent < 10.0 {
						percentPadding = "  "
					}
					if percent > 10 && percent != 100{
						percentPadding = " "
					}

					fmt.Printf(
						"[%*d/%d] %s%.2f %% done. lines in chunk: %d \n",
						len(strconv.Itoa(queue.chunkCount)),
						result.chunk.id,
						queue.chunkCount,
						percentPadding,
						percent,
						result.chunk.processedLines,
					)
				}

			case <-quit:
				return
			}
		}
	}()

	waitGroup.Wait()
	quit <- 0

	fmt.Println(strings.Repeat("-", 10))
	fmt.Printf(
		"took %v\nparsed %d lines\n%d Chunks failed:\n",
		time.Since(start),
		totalLines,
		len(failedChunks),
	)

	for _, failedChunk := range failedChunks {
		fmt.Printf("chunk %d failed: %s\n", failedChunk.chunk.id, failedChunk.err)
	}
}
