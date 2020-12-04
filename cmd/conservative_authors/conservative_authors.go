package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"time"
)

const conservativeSub = "Conservative"

func main() {
	csvfile, err := os.Open("out/2015_09-12.csv")
	if err != nil {
		log.Fatalln("Couldn't open the csv file", err)
	}

	reader := csv.NewReader(bufio.NewReader(csvfile))

	var conservativeAuthors = make(map[string]int)
	start := time.Now()

	record, err := reader.Read()
	if err != nil {
		log.Fatal(err)
	}

	var (
		subredditName = "subreddit"
		authorName    = "author"

		subredditKey int
		authorKey    int
	)

	for i, name := range record {
		if name == subredditName {
			subredditKey = i
		}

		if name == authorName {
			authorKey = i
		}
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}

		if err != nil {
			log.Fatal(err)
		}

		subreddit := record[subredditKey]
		author := record[authorKey]

		if subreddit == conservativeSub {
			value, ok := conservativeAuthors[author]
			if ok {
				conservativeAuthors[author] = value + 1
			} else {
				conservativeAuthors[author] = 1
			}
		}

	}

	fmt.Printf("took %v to parse\n", time.Since(start))

	path := "out/conservative_authors.csv"

	f, err := os.Create(path)
	if err != nil {
		log.Println(err)
	}
	defer f.Close()

	if _, err := f.WriteString(fmt.Sprintf("%v,%v\n", "author", "comment_count")); err != nil {
		log.Fatal(err)
	}

	total := 0
	for key, value := range conservativeAuthors {
		total = total + value

		if _, err := f.WriteString(fmt.Sprintf("%v,%v\n", key, value)); err != nil {
			log.Println(err)
		}
	}

	fmt.Printf("rednecks: %v\n", len(conservativeAuthors))
	fmt.Printf("comments: %v\n", total)
	fmt.Printf("comments per redneck: %v\n", total/len(conservativeAuthors))
	fmt.Printf("writing results to: %v\n", path)
}
