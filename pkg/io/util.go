package file

import (
	"io/ioutil"
)

func GetAllFilesInDir(dir string) ([]string, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var result []string
	for _, file := range files {
		if !file.IsDir() {
			result = append(result, file.Name())
		}
	}

	return result, nil
}

