// +build !windows

package utils

import (
	"errors"
	"os"
	"syscall"
)

var DirPath = "/"

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
	if info.Mode().Perm() & (1 << (uint(7))) == 0 {
		return errors.New("write permission bit is not set on this file for user")
	}

	//Check if path can be read
	var stat syscall.Stat_t
	if err = syscall.Stat(path, &stat); err != nil {
		return errors.New("unable to read path")
	}

	//Check if user has permissions to write to directory
	err = nil
	if uint32(os.Geteuid()) != stat.Uid {
		return errors.New("User doesn't have permission to write to this directory")
	}

	//Path is good
	return nil
}