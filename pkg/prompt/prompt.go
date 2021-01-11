package prompt

import (
	"fmt"
	"github.com/AlecAivazis/survey/v2"
	"github.com/fgehrlicher/reddit-comments/pkg/comment"
	"github.com/fgehrlicher/reddit-comments/pkg/io"
	"path"
)

type Result struct {
	InputFiles []string
	OutputFile string
	Fields     []string
}

func Run() (*Result, error) {
	dataDir, err := dataDir()
	if err != nil {
		return nil, fmt.Errorf("data dir prompt: %w", err)
	}

	inputFiles, err := inputFiles(dataDir)
	if err != nil {
		return nil, fmt.Errorf("input files prompt: %w", err)
	}

	fields, err := fields()
	if err != nil {
		return nil, fmt.Errorf("input files prompt: %w", err)
	}

	outFile, err := outputFile()
	if err != nil {
		return nil, fmt.Errorf("output file prompt: %w", err)
	}

	return &Result{
		InputFiles: inputFiles,
		OutputFile: outFile,
		Fields:     fields,
	}, nil
}

func dataDir() (string, error) {
	var dataDir string

	err := survey.AskOne(
		&survey.Input{
			Message: "data dir:",
		},
		&dataDir,
	)

	return dataDir, err
}

func inputFiles(dataDir string) ([]string, error) {
	files, err := io.GetAllFilesInDir(dataDir)
	if err != nil {
		return nil, err
	}

	var inputFiles []string

	err = survey.AskOne(
		&survey.MultiSelect{
			Message: "input file(s):",
			Options: files,
		},
		&inputFiles,
	)

	for i := range inputFiles {
		inputFiles[i] = path.Join(dataDir, inputFiles[i])
	}

	return inputFiles, err
}

func fields() ([]string, error) {
	var fields []string

	err := survey.AskOne(
		&survey.MultiSelect{
			Message: "fields to consider:",
			Options: comment.GetAllFields(),
		},
		&fields,
	)

	return fields, err
}

func outputFile() (string, error) {
	var outputFile string

	err := survey.AskOne(
		&survey.Input{
			Message: "result filename:",
		},
		&outputFile,
	)

	return outputFile, err
}
