package convert

import (
	"bytes"
	"errors"
	"io"
	"os"
	"sync"
)

// Chosen by fair dice roll.
const overflowIncrement = 1024

//
// All buffs and handles are kept allocated but wiped clean
// after each iteration of Worker.Process.
type Worker struct {
	TasksChan  chan Chunk
	resultChan chan ChunkResult
	waitGroup  *sync.WaitGroup
	chunkSize  int64

	handle       *os.File
	chunk        *Chunk
	buffHead     int
	buff         []byte
	overflowBuff []byte
	csvBuff      []byte
}

// NewWorker returns a new Worker whose buffer has the default size.
func NewWorker(tasks chan Chunk, result chan ChunkResult, chunkSize int64, waitGroup *sync.WaitGroup) *Worker {
	return &Worker{
		TasksChan:    tasks,
		resultChan:   result,
		waitGroup:    waitGroup,
		chunkSize:    chunkSize,
		buffHead:     0,
		buff:         make([]byte, chunkSize),
		overflowBuff: make([]byte, overflowIncrement),
		csvBuff:      make([]byte, chunkSize),
	}
}

// processes chunks from Worker.TasksChan until queue is empty
func (worker *Worker) Work() {
	defer worker.waitGroup.Done()
	var err error

	for chunk := range worker.TasksChan {
		worker.chunk = &chunk

		err = worker.Process()
		worker.resultChan <- ChunkResult{
			chunk: *worker.chunk,
			err: err,
		}
	}
}

var (
	ErrNotPartialLastLineNoLinebreak = errors.New("no linebreak but isn't a partial last line")
	ErrPartialFirstLineNoLinebreak   = errors.New("no linebreak found in buff but partial first line")
)

func (worker *Worker) Process() error {
	defer worker.resetBuffers()

	var err error

	err = worker.prepareFileHandles()
	if err != nil {
		return err
	}

	err = worker.readChunkInBuff()
	if err != nil {
		return err
	}

	err = worker.prepareBuff()
	if err != nil {
		return err
	}

	err = worker.convertBuffToCsv()
	if err != nil {
		return err
	}

	err = worker.writeCsvBuff()
	if err != nil {
		return err
	}

	return nil
}

func (worker *Worker) prepareBuff() error {
	worker.chunk.partialFirstLine = worker.buff[0] != '{'
	worker.chunk.partialLastLine = worker.buff[worker.chunk.realSize-1] != '\n'

	if worker.chunk.partialFirstLine {
		i := bytes.IndexByte(worker.buff, '\n')
		if i == -1 {
			return ErrPartialFirstLineNoLinebreak
		}

		worker.buffHead += i + 1
	}

	if worker.chunk.partialLastLine {
		err := worker.readOverflowInBuff()
		if err != nil {
			return err
		}
	}

	return nil
}

func (worker *Worker) prepareFileHandles() (err error) {
	if worker.handle == nil || worker.handle.Name() != worker.chunk.file {
		worker.handle, err = os.Open(worker.chunk.file)
	}

	_, err = worker.handle.Seek(worker.chunk.offset, io.SeekStart)
	return
}

// wipe all buffers but keep space allocated
func (worker *Worker) resetBuffers() {
	worker.buff = make([]byte, worker.chunkSize)
	worker.overflowBuff = make([]byte, overflowIncrement)
	worker.csvBuff = make([]byte, worker.chunkSize)
	worker.buffHead = 0
}

func (worker *Worker) readChunkInBuff() (err error) {
	worker.chunk.realSize, err = worker.handle.Read(worker.buff)
	if err != nil {
		return err
	}

	return
}

func (worker *Worker) readOverflowInBuff() error {
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
		scanMax += overflowIncrement
	}

	return nil
}

func (worker *Worker) convertBuffToCsv() error {
	var (
		line    []byte
		csvHead = 0
	)

	for {
		relativeIndex := bytes.IndexByte(worker.buff[worker.buffHead:], '\n')
		lastLine := relativeIndex == -1

		if lastLine && !worker.chunk.partialLastLine {
			return ErrNotPartialLastLineNoLinebreak
		}

		if lastLine {
			remainingBuff := worker.buff[worker.buffHead:]
			line = make([]byte, len(remainingBuff)+len(worker.overflowBuff))
			copy(line[:len(remainingBuff)], remainingBuff)
			copy(line[len(remainingBuff):], worker.overflowBuff)

			csvLine := worker.extractJson(line)
			copy(worker.csvBuff[csvHead:], csvLine)
			worker.csvBuff = worker.csvBuff[:csvHead+len(csvLine)]
			worker.chunk.processedLines++

			break
		}

		csvLine := worker.extractJson(worker.buff[worker.buffHead : worker.buffHead+relativeIndex])
		copy(worker.csvBuff[csvHead:], csvLine)

		csvHead += len(csvLine)
		worker.buffHead += relativeIndex + 1
		line = line[:0]
		worker.chunk.processedLines++

		if worker.buffHead == worker.chunk.realSize {
			break
		}
	}

	return nil
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

func (worker *Worker) writeCsvBuff() (err error) {
	_, err = worker.chunk.out.Write(worker.csvBuff)
	return
}
