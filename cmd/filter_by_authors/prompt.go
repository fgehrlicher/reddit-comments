package main

import (
	"github.com/AlecAivazis/survey/v2"
	"github.com/fgehrlicher/reddit-comments/pkg/io"
	"strings"
)

type AuthorsPromptResult struct {
	InputFile      string
	Subreddits     []string
}

func GetAuthorsPrompt() (*AuthorsPromptResult, error) {
	var (
		dataDir    string
		inputFile  string
	)

	dataDirPrompt := &survey.Input{
		Message: "data dir:",
	}
	err := survey.AskOne(dataDirPrompt, &dataDir)
	if err != nil {
		return nil, err
	}

	files, err := io.GetAllFiles(dataDir)
	if err != nil {
		return nil, err
	}

	filesSelectionPrompt := &survey.Select{
		Message: "input csv file:",
		Options: files,
	}
	err = survey.AskOne(filesSelectionPrompt, &inputFile)
	if err != nil {
		return nil, err
	}

	text := ""
	prompt := &survey.Multiline{
		Message: "subreddits",
	}
	err = survey.AskOne(prompt, &text)
	if err != nil {
		return nil, err
	}

	subreddits := strings.Split(text, "\n")

	return &AuthorsPromptResult{
		InputFile:      inputFile,
		Subreddits:     subreddits,
	}, nil
}
