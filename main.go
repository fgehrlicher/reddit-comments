package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"
)

type Comment struct {
	sub           string      `json:"sub"`
	Controversiality    int         `json:"controversiality"`
	AuthorFlairCSSClass interface{} `json:"author_flair_css_class"`
	Body                string      `json:"body"`
	CreatedUtc          string      `json:"created_utc"`
	Author              string      `json:"author"`
	Score               int         `json:"score"`
	Ups                 int         `json:"ups"`
	ID                  string      `json:"id"`
	ParentID            string      `json:"parent_id"`
	subID         string      `json:"sub_id"`
	RetrievedOn         int         `json:"retrieved_on"`
	Gilded              int         `json:"gilded"`
	Distinguished       interface{} `json:"distinguished"`
	LinkID              string      `json:"link_id"`
	AuthorFlairText     interface{} `json:"author_flair_text"`
}

func main() {
	file, err := os.Open("RC_2015-11")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	var subCommentCount = make(map[string]int)
	start := time.Now()

	for scanner.Scan() {
		line := scanner.Text()

		var comment Comment
		err := json.Unmarshal([]byte(line), &comment)

		if err != nil {
			log.Print(err)
			continue
		}

		value, ok := subCommentCount[comment.sub]
		if ok {
			subCommentCount[comment.sub] = value + 1
		} else {
			subCommentCount[comment.sub] = 1
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("took %v to parse\n", time.Since(start))
	fmt.Printf("subs: %v\n", len(subCommentCount))

	f, err := os.OpenFile("comments_per_sub",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	defer f.Close()

	for key, value := range subCommentCount {
		if _, err := f.WriteString(fmt.Sprintf("%v:%v\n", key, value)); err != nil {
			log.Println(err)
		}
	}
}
