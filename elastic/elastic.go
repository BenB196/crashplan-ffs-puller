package elastic

import (
	"errors"
	"strings"
)

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