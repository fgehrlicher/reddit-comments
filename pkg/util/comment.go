package util

type Comment struct {
	Subreddit           string      `json:"subreddit"`
	Controversiality    int         `json:"controversiality"`
	AuthorFlairCSSClass interface{} `json:"author_flair_css_class"`
	Body                string      `json:"body"`
	CreatedUtc          string      `json:"created_utc"`
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

