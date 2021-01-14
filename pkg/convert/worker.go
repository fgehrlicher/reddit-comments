package convert

import (
	"bytes"
	"errors"
	"io"
	"os"
	"sync"
)

// Chosen by fair dice roll.
const overflowScanSize = 1024

// All buffs and handles are kept allocated for all iterations of Worker.Process.
type Worker struct {
	TasksChan  chan Chunk
	resultChan chan ChunkResult
	waitGroup  *sync.WaitGroup
	chunkSize  int64

	handle       *os.File
	chunk        *Chunk
	buff         []byte
	overflowBuff []byte
	csvBuff      []byte
	buffHead     int
	csvBuffHead  int
}

// NewWorker returns a new Worker whose buffer has the default size.
func NewWorker(tasks chan Chunk, result chan ChunkResult, chunkSize int64, waitGroup *sync.WaitGroup) *Worker {
	return &Worker{
		TasksChan:    tasks,
		resultChan:   result,
		waitGroup:    waitGroup,
		chunkSize:    chunkSize,
		buff:         make([]byte, chunkSize),
		overflowBuff: make([]byte, overflowScanSize),
		csvBuff:      make([]byte, chunkSize),
		buffHead:     0,
		csvBuffHead:  0,
	}
}

// Work processes chunks from Worker.TasksChan until queue is empty
func (worker *Worker) Work() {
	defer worker.waitGroup.Done()
	var err error

	for chunk := range worker.TasksChan {
		worker.chunk = &chunk

		err = worker.Process()
		worker.resultChan <- ChunkResult{
			chunk: *worker.chunk,
			err:   err,
		}
	}
}

var (
	ErrNotPartialLastLineButIncompleteLine = errors.New("no linebreak but isn't a partial last line")
	ErrPartialOnlyOneIncompleteLine        = errors.New("no linebreak found in buff but partial first line")
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
			return ErrPartialOnlyOneIncompleteLine
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

// prepareFileHandles creates the main read handle and sets
// the read offset.
func (worker *Worker) prepareFileHandles() (err error) {
	if worker.handle == nil || worker.handle.Name() != worker.chunk.file {
		worker.handle, err = os.Open(worker.chunk.file)
	}

	_, err = worker.handle.Seek(worker.chunk.offset, io.SeekStart)
	return
}

// resetBuffers extend the size of all buffers to their cap and
// resets all buffer heads.
func (worker *Worker) resetBuffers() {
	worker.buff = worker.buff[:cap(worker.buff)]
	worker.overflowBuff = worker.overflowBuff[:cap(worker.overflowBuff)]
	worker.csvBuff = worker.csvBuff[:cap(worker.csvBuff)]
	worker.buffHead = 0
	worker.csvBuffHead = 0
}

// readChunkInBuff reads up to len(worker.buff) bytes from the file.
func (worker *Worker) readChunkInBuff() (err error) {
	worker.chunk.realSize, err = worker.handle.Read(worker.buff)
	return
}

// readOverflowInBuff reads overflowScanSize chunks until the next
// linebreak has been found.
func (worker *Worker) readOverflowInBuff() error {
	var (
		buffHead = 0
		buffSize = len(worker.overflowBuff)
	)

	for {
		scanBuff := worker.overflowBuff[buffHead:buffSize]

		if _, err := worker.handle.Read(scanBuff); err != nil {
			return err
		}

		i := bytes.IndexByte(scanBuff, '\n')
		if i > 0 {
			worker.overflowBuff = worker.overflowBuff[:buffHead+i]
			break
		}

		buffHead = buffSize
		buffSize += overflowScanSize
		newBuff := make([]byte, buffSize)

		copy(newBuff, worker.overflowBuff)
		worker.overflowBuff = newBuff
	}

	return nil
}

func (worker *Worker) convertBuffToCsv() error {
	var line []byte

	for {
		relativeIndex := bytes.IndexByte(worker.buff[worker.buffHead:], '\n')
		incompleteLine := relativeIndex == -1

		if incompleteLine && !worker.chunk.partialLastLine {
			return ErrNotPartialLastLineButIncompleteLine
		}

		if incompleteLine {
			remainingBuff := worker.buff[worker.buffHead:]
			line = make([]byte, len(remainingBuff)+len(worker.overflowBuff))
			copy(line[:len(remainingBuff)], remainingBuff)
			copy(line[len(remainingBuff):], worker.overflowBuff)

			csvLine := worker.extractJson(line)
			copy(worker.csvBuff[worker.csvBuffHead:], csvLine)
			worker.csvBuff = worker.csvBuff[:worker.csvBuffHead+len(csvLine)]
			worker.chunk.processedLines++

			break
		}

		csvLine := worker.extractJson(worker.buff[worker.buffHead : worker.buffHead+relativeIndex])
		copy(worker.csvBuff[worker.csvBuffHead:], csvLine)

		worker.csvBuffHead += len(csvLine)
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
