package convert

import (
	"io"
	"os"
)

type Chunk struct {
	id     int
	file   string
	offset int64
	size   int64

	out io.Writer
}

func SplitFileInChunks(chunkSize int64, fileIn string, fileOut io.Writer) ([]Chunk, error) {
	var (
		currentOffset int64 = 0
		currentChunk        = 1
		chunks        []Chunk
	)

	info, err := os.Stat(fileIn)
	if err != nil {
		return nil, err
	}

	for currentOffset <= info.Size() {
		chunks = append(chunks, Chunk{
			id:     currentChunk,
			offset: currentOffset,
			size:   chunkSize,
			file:   fileIn,
			out:    fileOut,
		})

		currentOffset += chunkSize
		currentChunk++
	}

	return chunks, err
}
