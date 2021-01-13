package main

import (
	"fmt"
	"log"
	"os"
)

func main()  {
	// You'll often want more control over how and what
	// parts of a file are read. For these tasks, start
	// by `Open`ing a file to obtain an `os.File` value.
	f, err := os.Open("data/test_10gb")
	check(err)

	// Read some bytes from the beginning of the file.
	// Allow up to 5 to be read but also note how many
	// actually were read.
	b1 := make([]byte, 5)
	n1, err := f.Read(b1)
	check(err)
	fmt.Printf("%d bytes: %s\n", n1, string(b1[:n1]))

	b1 = b1[:0]
	// You can also `Seek` to a known location in the file
	// and `Read` from there.
	o2, err := f.Seek(6, 0)
	check(err)
	n2, err := f.Read(b1)
	check(err)
	fmt.Printf("%d bytes @ %d: ", n2, o2)
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}