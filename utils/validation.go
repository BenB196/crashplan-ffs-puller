package utils

import (
	"errors"
	"regexp"
	"strings"
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

//Based of comments here: https://discuss.elastic.co/t/index-name-type-name-and-field-name-rules/133039
func ValidateIndexName(indexName string) error {
	if indexName == "" {
		return errors.New("error: index name cannot empty")
	}

	if strings.IndexAny(indexName,"ABCDEFGHIJKLMNOPQRSTUVWXYZ") > 0 {
		return errors.New("error: index name cannot contain any capitalized letters")
	}

	if strings.Contains(indexName,"\\") {
		return errors.New("error: index name cannot contain \"\\\"")
	}

	if strings.Contains(indexName,"/") {
		return errors.New("error: index name cannot contain \"/\"")
	}

	if strings.Contains(indexName,"*") {
		return errors.New("error: index name cannot contain \"*\"")
	}

	if strings.Contains(indexName,"?") {
		return errors.New("error: index name cannot contain \"?\"")
	}

	if strings.Contains(indexName,"\"") {
		return errors.New("error: index name cannot contain \"\"\"")
	}

	if strings.Contains(indexName,"<") {
		return errors.New("error: index name cannot contain \"<\"")
	}

	if strings.Contains(indexName,">") {
		return errors.New("error: index name cannot contain \">\"")
	}

	if strings.Contains(indexName,"|") {
		return errors.New("error: index name cannot contain \"|\"")
	}

	if strings.Contains(indexName," ") {
		return errors.New("error: index name cannot contain spaces")
	}

	if strings.HasPrefix(indexName,"_") {
		return errors.New("error: index name cannot start with \"_\"")
	}

	if strings.HasPrefix(indexName,"-") {
		return errors.New("error: index name cannot start with \"-\"")
	}

	if strings.HasPrefix(indexName,"+") {
		return errors.New("error: index name cannot start with \"+\"")
	}

	if indexName == "." {
		return errors.New("error: index name cannot be \".\"")
	}
	if indexName == ".." {
		return errors.New("error: index name cannot be \"..\"")
	}

	if len(indexName) > 255 {
		return errors.New("error: index name cannot be longer than 255 characters")
	}

	return nil
}