package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/fgehrlicher/reddit-comments/pkg/comment"
	"github.com/fgehrlicher/reddit-comments/pkg/io"
)

const conservativeSub = "Conservative"

func main() {
	scanner, err := io.LoadFile("data/RC_2015-11.json")
	if err != nil {
		log.Fatal(err)
	}

	var conservativeAuthors = make(map[string]int)
	start := time.Now()

	for scanner.Scan() {
		line := scanner.Text()

		var com comment.Comment
		err := json.Unmarshal([]byte(line), &com)

		if err != nil {
			log.Print(err)
			continue
		}

		if com.AuthorFlairText == nil {
			continue
		}

		authorFlairText := fmt.Sprintf("%v", com.AuthorFlairText)

		if com.Subreddit == conservativeSub && authorFlairText != "" {
			value, ok := conservativeAuthors[com.Author]
			if ok {
				conservativeAuthors[com.Author] = value + 1
			} else {
				conservativeAuthors[com.Author] = 1
			}
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("took %v to parse\n", time.Since(start))

	path := "out/flaired_rednecks.csv"

	f, err := os.Create(path)
	if err != nil {
		log.Println(err)
	}
	defer f.Close()

	if _, err := f.WriteString(fmt.Sprintf("%v,%v\n", "redneck", "comment_count")); err != nil {
		log.Fatal(err)
	}

	total := 0
	for key, value := range conservativeAuthors {
		total = total + value

		if _, err := f.WriteString(fmt.Sprintf("%v,%v\n", key, value)); err != nil {
			log.Println(err)
		}
	}

	fmt.Printf("flaired rednecks: %v\n", len(conservativeAuthors))
	fmt.Printf("comments: %v\n", total)
	fmt.Printf("comments per flaired redneck: %v\n", total/len(conservativeAuthors))
	fmt.Printf("writing results to: %v\n", path)
}
