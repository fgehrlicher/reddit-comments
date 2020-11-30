package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/fgehrlicher/reddit-comments/pkg/util"
	"log"
	"os"
	"time"
)

const conservativeSub = "Conservative"

func main() {
	file, err := os.Open("RC_2015-11")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	var conservativeAuthors = make(map[string]int)
	start := time.Now()

	for scanner.Scan() {
		line := scanner.Text()

		var comment util.Comment
		err := json.Unmarshal([]byte(line), &comment)

		if err != nil {
			log.Print(err)
			continue
		}

		if comment.AuthorFlairText == nil {
			continue
		}

		text := fmt.Sprintf("%v", comment.AuthorFlairText)


		if comment.Subreddit == conservativeSub && text != "" {

			value, ok := conservativeAuthors[comment.Author]
			if ok {
				conservativeAuthors[comment.Author] = value + 1
			} else {
				conservativeAuthors[comment.Author] = 1
			}

		}

	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("took %v to parse\n", time.Since(start))

	f, err := os.OpenFile("rednecks",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	defer f.Close()

	total := 0
	for key, value := range conservativeAuthors {
		total = total + value

		if _, err := f.WriteString(fmt.Sprintf("%v:%v\n", key, value)); err != nil {
			log.Println(err)
		}
	}

	fmt.Printf("flaired rednecks: %v\n", len(conservativeAuthors))
	fmt.Printf("comments: %v\n", total)
	fmt.Printf("comments per flaired redneck: %v\n", total/len(conservativeAuthors))

}
