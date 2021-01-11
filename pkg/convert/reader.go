package convert

import (
	"bytes"
	"io"
)

func ReadUntilLinebreak(handle io.ReaderAt, offset int64) ([]byte, error) {
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
