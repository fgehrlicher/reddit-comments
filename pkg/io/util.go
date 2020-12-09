package io

import (
	"fmt"
	"os"
	"path/filepath"
)

func GetAllFiles(base string) ([]string, error) {
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
