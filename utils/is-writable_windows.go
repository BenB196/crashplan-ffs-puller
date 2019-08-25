package utils

import (
	"errors"
	"os"
)

var DirPath = "\\"

func IsWritable(path string) error {

	//Check if path exists
	info, err := os.Stat(path)
	if err != nil {
		return errors.New("path: " + path + " doesn't exist")
	}

	//Check if path is a directory
	err = nil
	if !info.IsDir() {
		return errors.New("path: " + path + " isn't a directory")
	}

	// Check if the user bit is enabled in file permission
	if info.Mode().Perm()&(1<<(uint(7))) == 0 {
		return errors.New("write permission bit is not set on this file for user")
	}

	return nil
}