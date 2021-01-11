package convert

import (
	"bufio"
	"bytes"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"io"
	"os"
	"sync"
)

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
		missingLine, err := ReadUntilLinebreak(handle, chunk.offset+chunk.size)
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
