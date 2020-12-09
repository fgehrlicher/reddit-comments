package prompt

import (
	"github.com/AlecAivazis/survey/v2"
	"github.com/fgehrlicher/reddit-comments/pkg/comment"
	"github.com/fgehrlicher/reddit-comments/pkg/io"
)

type promptResult struct {
	FilesToConvert []string
	FieldsToCovert []string
	ResultFilename string
}

func Prompt() (*promptResult, error) {
	var (
		dataDir        string
		filesToConvert []string
		fieldsToCovert []string
		resultFilename string
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

	filesSelectionPrompt := &survey.MultiSelect{
		Message: "files to convert:",
		Options: files,
	}
	err = survey.AskOne(filesSelectionPrompt, &filesToConvert)
	if err != nil {
		return nil, err
	}

	fieldsToCovertPrompt := &survey.MultiSelect{
		Message: "fields to consider:",
		Options: comment.GetAllFields(),
	}
	err = survey.AskOne(fieldsToCovertPrompt, &fieldsToCovert)
	if err != nil {
		return nil, err
	}

	resultFilenamePrompt := &survey.Input{
		Message: "result filename:",
	}
	err = survey.AskOne(resultFilenamePrompt, &resultFilename)
	if err != nil {
		return nil, err
	}

	return &promptResult{
		FilesToConvert: filesToConvert,
		ResultFilename: resultFilename,
		FieldsToCovert: fieldsToCovert,
	}, nil
}
