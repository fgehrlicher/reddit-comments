.PHONY: help
help:
	@grep '^.PHONY: .* #' Makefile | sed 's/\.PHONY: \(.*\) # \(.*\)/\1 \2/' | expand -t20

.PHONY: comments-per-subreddit # - Generates the comments per subreddit csv file
comments-per-subreddit:
	go run cmd/comments_per_subreddit/main.go .

.PHONY: conservative_authors # - Generates the comments per subreddit csv file
conservative_authors:
	go run cmd/conservative_authors/main.go .
