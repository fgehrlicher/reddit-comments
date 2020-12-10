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

func main() {
	promptResult, err := GetAuthorsPrompt()
	if err != nil {
		log.Fatalln("prompt err")
	}

	csvfile, err := os.Open(promptResult.InputFile)
	if err != nil {
		log.Fatalln("Couldn't open the csv file", err)
	}

	reader := csv.NewReader(bufio.NewReader(csvfile))
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

	var authors []string
	var commentCount = make(map[string]int)

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

		if inSlice(promptResult.Subreddits, subreddit) {

			value, ok := commentCount[subreddit]
			if ok {
				commentCount[subreddit] = value + 1
			} else {
				commentCount[subreddit] = 1
			}

			if !inSlice(authors, author) {
				authors = append(authors, author)
			}
		}

	}

	fmt.Printf("took %v to parse\n", time.Since(start))
	for key, value := range commentCount {
		fmt.Printf("comment count for %v: %v \n", key, value)
	}

	f, err := os.Create(promptResult.ResultFilename)
	if err != nil {
		log.Println(err)
	}
	defer f.Close()

	for _, author := range authors {

		if _, err := f.WriteString(fmt.Sprintf("%v\n", author)); err != nil {
			log.Println(err)
		}
	}

	fmt.Printf("writing results to: %v\n", "outputFile")
}

func inSlice(haystack []string, needle string) bool {
	for _, item := range haystack {
		if item == needle {
			return true
		}
	}

	return false
}
