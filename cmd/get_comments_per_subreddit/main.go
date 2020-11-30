package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/fgehrlicher/reddit-comments/pkg/util"
)

func main() {
	file, err := os.Open("RC_2015-11")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	var subredditCommentCount = make(map[string]int)
	start := time.Now()

	for scanner.Scan() {
		line := scanner.Text()

		var comment util.Comment
		err := json.Unmarshal([]byte(line), &comment)

		if err != nil {
			log.Print(err)
			continue
		}

		value, ok := subredditCommentCount[comment.Subreddit]
		if ok {
			subredditCommentCount[comment.Subreddit] = value + 1
		} else {
			subredditCommentCount[comment.Subreddit] = 1
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("took %v to parse\n", time.Since(start))
	fmt.Printf("subreddits: %v\n", len(subredditCommentCount))

	f, err := os.OpenFile("comments_per_subreddit",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	defer f.Close()

	for key, value := range subredditCommentCount {
		if _, err := f.WriteString(fmt.Sprintf("%v:%v\n", key, value)); err != nil {
			log.Println(err)
		}
	}
}
