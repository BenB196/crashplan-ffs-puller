package utils

import (
	"errors"
	"regexp"
)

func ValidateUsernameRegexp(username string) error {
	var regexUsername = regexp.MustCompile("^[a-zA-Z0-9.!#$%&'*+\\/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$")
	if len(username) > 254 {
		return errors.New("username is greater then 254 characters")
	} else if !regexUsername.MatchString(username) {
		return errors.New("username is invalid, username must be a valid email address")
	} else {
		return nil
	}
}