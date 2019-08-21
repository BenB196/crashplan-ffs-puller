package config

import (
	"crashplan-ffs-puller/utils"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"path/filepath"
)

type Config struct {
	Username 		string			`json:"username"`
	Password 		string 			`json:"password"`
	AuthURI  		string 			`json:"authURI"`
	FFSURI   		string 			`json:"ffsURI"`
	QueryInterval	string			`json:"queryInterval"`
	FFSQueries		[]string 		`json:"ffsQueries"`
}

//Read the configuration file and return the Config struct
func ReadConfig(configLocation string) (Config, error) {

	//Make sure absolute file path is gotten
	configLocation, err := filepath.Abs(configLocation)

	//error if abs path cannot be gotten
	if err != nil {
		return Config{}, errors.New("error getting absolute configuration file path: " + err.Error())
	}

	//open config file
	configFile, err := os.Open(configLocation)

	//return err if failure
	if err != nil {
		return Config{}, errors.New("error opening configuration file: " + err.Error())
	}

	//Convert file to byteValue
	byteValue, err := ioutil.ReadAll(configFile)

	//return err if failure
	if err != nil {
		return Config{}, errors.New("error reading configuration file: " + err.Error())
	}

	//Check file extension
	fileExtension := path.Ext(configLocation)

	//If no file extension err
	switch fileExtension {
	case "":
		return Config{}, errors.New("no file extension found on configuration file, unable to properly parse")
	case ".json",".JSON",".Json":
		return parseConfigJson(byteValue)
	case ".yaml",".YAML",".Yaml",".yml",".YML",".Yml":
		//TODO parse file as yaml
		return Config{}, nil
	default:
		return Config{}, errors.New("unknown file extension: " + fileExtension + ", supported file extensions: json, JSON, Json, yaml, YAML, Yaml, yml, YML, Yml")
	}
}

func parseConfigJson(fileBytes []byte) (Config, error) {
	//create config struct
	var config Config
	//Make sure file is valid JSON
	err := json.Unmarshal(fileBytes, &config)

	//return error if unmarshal fails
	if err != nil {
		return config, errors.New("error parsing JSON configuration file: " + err.Error())
	}

	//Validate JSON fields
	//Validate username: check if empty and is valid email
	if config.Username == "" {
		return config, errors.New("error, username in configuration file is blank")
	} else {
		err = utils.ValidateUsernameRegexp(config.Username)
		if err != nil {
			return config, err
		}
	}

	//Validate password: check if empty
	if config.Password == "" {
		return config, errors.New("error: password cannot be blank")
	}

	//Validate AuthURI: check if empty and is valid request uri
	if config.AuthURI == "" {
		return config, errors.New("error: authURI cannot be blank")
	} else {
		_, err := url.ParseRequestURI(config.AuthURI)
		if err != nil {
			return config, errors.New("error: bad authURI provided: " + err.Error())
		}
	}

	//Validate FFSURI: check if empty and is valid request uri
	if config.FFSURI == "" {
		return config, errors.New("error: FFSURI cannot be blank")
	} else {
		_, err := url.ParseRequestURI(config.FFSURI)
		if err != nil {
			return config, errors.New("error: bad FFSURI provided: " + err.Error())
		}
	}

	//TODO see if query interval needs to actually be validated or if it will be handled on unmarshal

	//TODO figure out how to best validate FFSQueries

	return config, nil
}