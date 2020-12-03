package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/AlecAivazis/survey/v2"
	"github.com/fgehrlicher/reddit-comments/pkg/comment"
	"github.com/fgehrlicher/reddit-comments/pkg/io"
)

func main() {
	promptResult, err := prompt()
	if err != nil {
		log.Fatal(err)
	}

	resultFile, err := os.Create(promptResult.ResultFilename)
	if err != nil {
		log.Fatal(err)
	}

	resultFileWriter := csv.NewWriter(resultFile)
	err = resultFileWriter.Write(promptResult.FieldsToCovert)
	if err != nil {
		log.Fatal(err)
	}

	resultFileWriter.Flush()
	fmt.Println("_________________________")

	for _, file := range promptResult.FilesToConvert {
		start := time.Now()
		fmt.Println(fmt.Sprintf("processing %s ...", file))

		scanner, err := io.LoadFile(file)
		if err != nil {
			log.Fatal(err)
		}

		for scanner.Scan() {
			line := scanner.Text()

			var com comment.Comment
			err := json.Unmarshal([]byte(line), &com)
			if err != nil {
				log.Println(fmt.Sprintf("error: %s", err.Error()))
				log.Println(fmt.Sprintf("full row: %s", line))
				continue
			}

			var row []string
			for _, field := range promptResult.FieldsToCovert {

				getter, err := comment.GetMapperForField(field)
				if err != nil {
					log.Fatal(err)
				}
				row = append(row, getter(&com))
			}

			err = resultFileWriter.Write(row)
			if err != nil {
				log.Fatal(err)
			}

			resultFileWriter.Flush()
		}
		elapsed := time.Since(start)
		fmt.Println(fmt.Sprintf("convert took %s", elapsed))
		fmt.Println("_________________________")
	}

}

type promptResult struct {
	FilesToConvert []string
	FieldsToCovert []string
	ResultFilename string
}

func prompt() (*promptResult, error) {
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
		log.Fatal(err)
	}

	files, err := getAllFiles(dataDir)
	if err != nil {
		return nil, err
	}

	filesSelectionPrompt := &survey.MultiSelect{
		Message: "files to convert:",
		Options: files,
	}
	err = survey.AskOne(filesSelectionPrompt, &filesToConvert)
	if err != nil {
		log.Fatal(err)
	}

	fieldsToCovertPrompt := &survey.MultiSelect{
		Message: "fields to consider:",
		Options: comment.GetAllFields(),
	}
	err = survey.AskOne(fieldsToCovertPrompt, &fieldsToCovert)
	if err != nil {
		log.Fatal(err)
	}

	resultFilenamePrompt := &survey.Input{
		Message: "result filename:",
	}
	err = survey.AskOne(resultFilenamePrompt, &resultFilename)
	if err != nil {
		log.Fatal(err)
	}

	return &promptResult{
		FilesToConvert: filesToConvert,
		ResultFilename: resultFilename,
		FieldsToCovert: fieldsToCovert,
	}, nil
}

func getAllFiles(base string) ([]string, error) {
	var files []string

	err := filepath.Walk(
		base,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			files = append(files, path)
			return nil
		},
	)
	if err != nil {
		return nil, err
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("no files found in '%s'", base)
	}

	return files, nil
}
