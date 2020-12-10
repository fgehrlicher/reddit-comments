package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/fgehrlicher/reddit-comments/pkg/comment"
	"github.com/fgehrlicher/reddit-comments/pkg/io"
)

func main() {
	promptResult, err := ConvertToCsvPrompt()
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
