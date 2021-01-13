package convert

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

type ProcessResult struct {
	chunk          Chunk
	processedLines int
	err            error
}

type Worker struct {
	tasks     chan Chunk
	result    chan ProcessResult
	waitGroup *sync.WaitGroup
	chunkSize int64

	handle            *os.File
	buff              []byte
	overflowBuff      []byte
	csvBuff           []byte
	overflowIncrement int
}

func NewWorker(tasks chan Chunk, result chan ProcessResult, chunkSize int64, waitGroup *sync.WaitGroup) *Worker {
	overflowIncrement := 1024
	return &Worker{
		tasks:             tasks,
		result:            result,
		waitGroup:         waitGroup,
		chunkSize:         chunkSize,
		buff:              make([]byte, chunkSize),
		overflowBuff:      make([]byte, overflowIncrement),
		overflowIncrement: overflowIncrement,
		//@TODO
		csvBuff: make([]byte, chunkSize),
	}
}

func (worker *Worker) Work() {
	defer worker.waitGroup.Done()

	for chunk := range worker.tasks {
		worker.process(chunk)
	}
}

func (worker *Worker) process(chunk Chunk) {
	defer worker.resetBuffers()

	var (
		result = ProcessResult{chunk: chunk}
		err    error
	)

	err = worker.prepareHandle(&chunk)
	if err != nil {
		worker.handleErr(result, err)
		return
	}

	realChunkSize, err := worker.handle.Read(worker.buff)
	if err != nil {
		worker.handleErr(result, err)
		return
	}

	if realChunkSize == 0 {
		worker.result <- result
		return
	}

	// process chunk
	var (
		partialFirstLine = worker.buff[0] != '{'
		partialLastLine  = worker.buff[realChunkSize-1] != '\n'
		buffHead         = 0
	)

	if partialFirstLine {
		i := bytes.IndexByte(worker.buff, '\n')
		if i == -1 {
			worker.handleErr(result, fmt.Errorf("no linebreak found in buff but partial first line"))
		}

		buffHead += i + 1
	}

	if partialLastLine {
		err = worker.fillOverflowBuffer()
		if err != nil {
			worker.handleErr(result, err)
			return
		}
	}

	var (
		line    []byte
		csvHead int
	)

	for {
		relativeIndex := bytes.IndexByte(worker.buff[buffHead:], '\n')
		lastLine := relativeIndex == -1

		if lastLine && !partialLastLine {
			chunkTest := worker.buff[buffHead-10:]
			err = errors.New("no linebreak but isn't a partial last line")
			worker.handleErr(result, err)
			_ = chunkTest
			return
		}

		if lastLine {
			remainingBuff := worker.buff[buffHead:]
			line = make([]byte, len(remainingBuff)+len(worker.overflowBuff))
			copy(line[:len(remainingBuff)], remainingBuff)
			copy(line[len(remainingBuff):], worker.overflowBuff)

			csvLine := worker.extractJson(line)
			copy(worker.csvBuff[csvHead:], csvLine)
			worker.csvBuff = worker.csvBuff[:csvHead+len(csvLine)]
			result.processedLines++

			break
		}

		csvLine := worker.extractJson(worker.buff[buffHead : buffHead+relativeIndex])
		copy(worker.csvBuff[csvHead:], csvLine)

		csvHead += len(csvLine)
		buffHead += relativeIndex + 1
		line = line[:0]
		result.processedLines++

		if buffHead == realChunkSize {
			break
		}
	}

	_, err = chunk.out.Write(worker.csvBuff)
	if err != nil {
		worker.handleErr(result, err)
		return
	}

	worker.result <- result
}

func (worker *Worker) prepareHandle(chunk *Chunk) (err error) {
	if worker.handle == nil || worker.handle.Name() != chunk.file {
		worker.handle, err = os.Open(chunk.file)
	}

	_, err = worker.handle.Seek(chunk.offset, io.SeekStart)
	return
}

func (worker *Worker) extractJson(data []byte) []byte {
	var (
		author    = worker.extractField(data, []byte("\"author\":\""), []byte("\",\""))
		subreddit = worker.extractField(data, []byte("\"subreddit\":\""), []byte("\",\""))
	)

	return []byte(string(author) + "," + string(subreddit) + "\n")
}

func (worker *Worker) extractField(data, fieldSelector, fieldSelectorEnd []byte) []byte {
	fieldStart := bytes.Index(data, fieldSelector) + len(fieldSelector)
	fieldEnd := bytes.Index(data[fieldStart:], fieldSelectorEnd) + fieldStart

	return data[fieldStart:fieldEnd]
}

func (worker *Worker) fillOverflowBuffer() error {
	var (
		buffHead = 0
		scans    = 0
		scanMax  = cap(worker.overflowBuff)
	)

	for {
		scans++

		if scanMax > cap(worker.overflowBuff) {
			newBuff := make([]byte, scanMax)
			copy(newBuff, worker.overflowBuff)
			worker.overflowBuff = newBuff
		}

		scanBuff := worker.overflowBuff[buffHead:scanMax]

		_, err := worker.handle.Read(scanBuff)
		if err != nil {
			return err
		}

		i := bytes.IndexByte(scanBuff, '\n')
		if i > 0 {
			worker.overflowBuff = worker.overflowBuff[:buffHead+i]
			break
		}

		buffHead = scanMax
		scanMax += worker.overflowIncrement
	}

	return nil
}

func (worker *Worker) handleErr(result ProcessResult, err error) {
	result.err = err
	worker.result <- result
}

func (worker *Worker) resetBuffers() {
	worker.buff = make([]byte, worker.chunkSize)
	worker.overflowBuff = make([]byte, worker.overflowIncrement)
	worker.csvBuff = make([]byte, worker.chunkSize)
}
