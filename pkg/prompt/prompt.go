package prompt

import (
	"fmt"
	"github.com/AlecAivazis/survey/v2"
	"github.com/fgehrlicher/reddit-comments/pkg/io"
)

type Result struct {
	DataDir     string
	InputFiles  []string `survey:"input files"`
	OutputFile  string `survey:"output io"`
	chunkSize   int64 `survey:"chunk size"`
	workerCount int `survey:"worker size"`
}

func getQuestions(dataDir string) ([]*survey.Question, error) {
	files, err := io.GetAllFilesInDir(dataDir)
	if err != nil {
		return nil, err
	}

	return []*survey.Question{
		{
			Name: "input files",
			Prompt: &survey.MultiSelect{
				Message: "select input io(s):",
				Options: files,
			},
		},
		{
			Name: "output io",
			Prompt: &survey.Input{
				Message: "result filename:",
			},
		},
		{
			Name: "chunk size",
			Prompt: &survey.Input{
				Message: "result filename:",
			},
		},
	}, nil
}




func Run() (*Result, error) {
	result := new(Result)

	if err := dataDirPrompt(result); err != nil {
		return nil, fmt.Errorf("data dir prompt: %w", err)
	}

	qs, err := getQuestions(result.DataDir)
	if err != nil {
		return nil, fmt.Errorf("cannot generate prompt questions: %w", err)
	}

	if err := survey.Ask(qs, result); err != nil {
		return nil, fmt.Errorf("cannot generate prompt questions: %w", err)
	}

	return result, err
}

func dataDirPrompt(result *Result) error {
	return survey.AskOne(&survey.Input{Message: "data dir:"}, result.DataDir)
}
