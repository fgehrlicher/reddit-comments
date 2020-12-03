package comment

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
)

type Comment struct {
	Subreddit           string      `json:"subreddit"`
	Controversiality    int         `json:"controversiality"`
	AuthorFlairCSSClass interface{} `json:"author_flair_css_class"`
	Body                string      `json:"body"`
	CreatedUtc          interface{} `json:"created_utc"`
	Author              string      `json:"author"`
	Score               int         `json:"score"`
	Ups                 int         `json:"ups"`
	ID                  string      `json:"id"`
	ParentID            string      `json:"parent_id"`
	SubredditID         string      `json:"subreddit_id"`
	RetrievedOn         int         `json:"retrieved_on"`
	Gilded              int         `json:"gilded"`
	Distinguished       interface{} `json:"distinguished"`
	LinkID              string      `json:"link_id"`
	AuthorFlairText     interface{} `json:"author_flair_text"`
}

type Getter func(*Comment) string

var fieldGetMap = map[string]Getter{
	"subreddit":              GetSubreddit,
	"controversiality":       GetControversiality,
	"author_flair_css_class": GetAuthorFlairCSSClass,
	"body":                   GetBody,
	"created_utc":            GetCreatedUtc,
	"author":                 GetAuthor,
	"score":                  GetScore,
	"ups":                    GetUps,
	"id":                     GetID,
	"parent_id":              GetParentID,
	"subreddit_id":           GetSubredditID,
	"retrieved_on":           GetRetrievedOn,
	"gilded":                 GetGilded,
	"distinguished":          GetDistinguished,
	"link_id":                GetLinkID,
	"author_flair_text":      GetAuthorFlairText,
}

func GetAllFields() []string {
	var result []string

	for key, _ := range fieldGetMap {
		result = append(result, key)
	}

	return result
}

func GetMapperForField(field string) (Getter, error) {
	getter, ok := fieldGetMap[field]
	if !ok {
		return nil, errors.New("no field with that name found")
	}

	return getter, nil
}

func GetSubreddit(comment *Comment) string {
	return comment.Subreddit
}

func GetControversiality(comment *Comment) string {
	return strconv.Itoa(comment.Controversiality)
}

func GetAuthorFlairCSSClass(comment *Comment) string {
	return base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%v", comment.AuthorFlairCSSClass)))
}

func GetBody(comment *Comment) string {
	return base64.StdEncoding.EncodeToString([]byte(comment.Body))
}

func GetCreatedUtc(comment *Comment) string {
	return fmt.Sprintf("%v", comment.CreatedUtc)
}

func GetAuthor(comment *Comment) string {
	return comment.Author
}

func GetScore(comment *Comment) string {
	return strconv.Itoa(comment.Score)
}

func GetUps(comment *Comment) string {
	return strconv.Itoa(comment.Ups)
}

func GetID(comment *Comment) string {
	return comment.ID
}

func GetParentID(comment *Comment) string {
	return comment.ParentID
}

func GetSubredditID(comment *Comment) string {
	return comment.SubredditID
}

func GetRetrievedOn(comment *Comment) string {
	return strconv.Itoa(comment.RetrievedOn)
}

func GetGilded(comment *Comment) string {
	return strconv.Itoa(comment.Gilded)
}

func GetDistinguished(comment *Comment) string {
	return fmt.Sprintf("%v", comment.Distinguished)
}

func GetLinkID(comment *Comment) string {
	return comment.LinkID
}

func GetAuthorFlairText(comment *Comment) string {
	return fmt.Sprintf("%v", comment.AuthorFlairText)
}
