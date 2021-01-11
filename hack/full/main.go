package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
)

func main() {
	//p := profile.Start(profile.TraceProfile, profile.ProfilePath("."), profile.NoShutdownHook)

	var (
		fileIn           = "data/RC_2019-10"
		fileOut          = "result.csv"
		chunkSize  int64 = 1024 * 1024 * 100 // 100 MiB
		maxThreads       = 10

		currentOffset int64 = 0
		currentChunk        = 1
		chunks        []*Chunk
	)

	info, err := os.Stat(fileIn)
	if err != nil {
		panic(err)
	}

	out, err := os.Create(fileOut)
	if err != nil {
		panic(err)
	}

	for currentOffset <= info.Size() {
		chunks = append(chunks, &Chunk{
			id:     currentChunk,
			offset: currentOffset,
			size:   chunkSize,
			file:   fileIn,
			out:    out,
		})

		currentOffset += chunkSize
		currentChunk++
	}

	start := time.Now()

	chunkHead := 0
	resultChan := make(chan ProcessResult, maxThreads)
	var wg sync.WaitGroup

	wg.Add(len(chunks))
	for i := 0; i < maxThreads; i++ {
		if chunkHead >= len(chunks) {
			break
		}

		go chunks[chunkHead].Process(resultChan, &wg)
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
						len(strconv.Itoa(len(chunks))),
						result.chunk.id,
						len(chunks),
						result.err,
					)
					failedChunks = append(failedChunks, result)

				} else {
					fmt.Printf(
						"[%*d/%d] done %.2f %%\n",
						len(strconv.Itoa(len(chunks))),
						result.chunk.id,
						len(chunks),
						float32(chunksProcessed)/float32(len(chunks))*100,
					)
				}

				if chunkHead < len(chunks) {
					go chunks[chunkHead].Process(resultChan, &wg)
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
	fmt.Printf("%d chunks failed:", len(failedChunks))
	for _, failedChunk := range failedChunks {
		fmt.Printf("%d chunks failed: %s", failedChunk.chunk.id, failedChunk.err)
	}

	//p.Stop()
}

type Chunk struct {
	id     int
	file   string
	offset int64
	size   int64

	out io.Writer
}

type ProcessResult struct {
	chunk Chunk
	err   error
}

func (chunk Chunk) Process(resultChan chan ProcessResult, wg *sync.WaitGroup) {
	defer wg.Done()
	result := ProcessResult{chunk: chunk}

	handle, err := os.Open(chunk.file)
	if err != nil {
		result.err = err
		resultChan <- result
		return
	}

	// get chunk
	buff := make([]byte, chunk.size)
	n, err := handle.ReadAt(buff, chunk.offset)
	if err != nil {
		if err == io.EOF {
			buff = buff[:n]
		} else {
			result.err = err
			resultChan <- result
			return
		}
	}

	// prepare chunk
	var (
		partialFirstLine = buff[0] != '{'
		partialLastLine  = buff[len(buff)-1] != '\n'
	)

	if partialLastLine {
		missingLine, err := readUntilLinebreak(handle, chunk.offset+chunk.size)
		if err != nil {
			result.err = err
			resultChan <- result
			return
		}

		buff = append(buff, missingLine...)
	}

	bufReader := bufio.NewReader(bytes.NewReader(buff))
	if partialFirstLine {
		_, _, err := bufReader.ReadLine()
		if err != nil {
			result.err = err
			resultChan <- result
			return
		}
	}

	csvResult := make([]byte, 0)

	for {
		line, err := bufReader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			result.err = err
			resultChan <- result
			return
		}

		comment := make(map[string]interface{})
		err = jsoniter.Unmarshal(line, &comment)
		if err != nil {
			result.err = err
			resultChan <- result
			return
		}
		convertedLine := fmt.Sprintf("%s,%s\n", comment["subreddit"], comment["author"])

		csvResult = append(csvResult, []byte(convertedLine)...)
	}

	_, err = chunk.out.Write(csvResult)
	if err != nil {
		result.err = err
		resultChan <- result
		return
	}

	resultChan <- result
}

func readUntilLinebreak(handle io.ReaderAt, offset int64) ([]byte, error) {
	var (
		currentHead  = offset
		allBuff      []byte
		scans        = 0
		scanBuffSize = 1024
	)

	for {
		scans++

		buff := make([]byte, scanBuffSize)
		_, err := handle.ReadAt(buff, currentHead)
		if err != nil && err != io.EOF {
			return nil, err
		}

		i := bytes.IndexByte(buff, '\n')
		if i > 0 {
			allBuff = append(allBuff, buff[:i+1]...)
			break
		} else {
			allBuff = append(allBuff, buff...)
		}

		currentHead += int64(scanBuffSize)
	}

	//fmt.Printf("took %d additional scans\n", scans)
	return allBuff, nil
}
