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

func main() {
	scanner, err := io.LoadFile("data/RC_2015-11.json")
	if err != nil {
		log.Fatal(err)
	}

	var subredditCommentCount = make(map[string]int)
	start := time.Now()

	for scanner.Scan() {
		line := scanner.Text()

		var com comment.Comment
		err := json.Unmarshal([]byte(line), &com)

		if err != nil {
			log.Print(err)
			continue
		}

		value, ok := subredditCommentCount[com.Subreddit]
		if ok {
			subredditCommentCount[com.Subreddit] = value + 1
		} else {
			subredditCommentCount[com.Subreddit] = 1
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	outPath := "out/comments_per_subreddit.csv"

	fmt.Printf("took %v to parse\n", time.Since(start))
	fmt.Printf("sub count: %v\n", len(subredditCommentCount))
	fmt.Printf("writing to: %v\n", outPath)

	f, err := os.Create(outPath)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	if _, err := f.WriteString(fmt.Sprintf("%v,%v\n", "subreddit", "comment_count")); err != nil {
		log.Fatal(err)
	}

	for key, value := range subredditCommentCount {
		if _, err := f.WriteString(fmt.Sprintf("%v,%v\n", key, value)); err != nil {
			log.Fatal(err)
		}
	}
}
