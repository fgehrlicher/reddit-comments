package io

import (
	"bufio"
	"os"
)

func LoadFile(path string) (*bufio.Scanner, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(file)

	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	return scanner, nil
}
