package main

import (
	"fmt"
	"io"
	"log"
	"os"
)

func main() {
	buff := make([]byte, 1024)

	file, err := os.Open("data/test_10gb")
	handleErr(err)

	n, err := file.Read(buff)
	handleErr(err)
	fmt.Printf("read %d bytes\n", n)
	buff = make([]byte, 1024)

	newPos, err := file.Seek(0, io.SeekStart)
	fmt.Printf("new pos: %d\n", newPos)

	n, err = file.Read(buff)
	handleErr(err)
	fmt.Printf("read %d bytes\n", n)
	buff = buff[:0]

}

func handleErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
