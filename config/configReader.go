package config

import (
	ffs "crashplan-ffs-go-pkg"
	"crashplan-ffs-puller/utils"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"time"
)

type Config struct {
	AuthURI  		string 			`json:"authURI"`
	FFSURI   		string 			`json:"ffsURI"`
	FFSQueries		[]FFSQuery		`json:"ffsQueries"`
}

type FFSQuery struct {
	Username 		string			`json:"username"`
	Password 		string 			`json:"password"`
	QueryInterval	string			`json:"queryInterval"`
	Query			ffs.Query 		`json:"query"`
	OutputLocation  string			`json:"outputLocation,omitempty"`
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

	if config.FFSQueries == nil {
		return config, errors.New("error: no ffs queries provided")
	} else {
		for queryNumber, query := range config.FFSQueries {
			//convert queryNumber to string
			queryNumberString := strconv.Itoa(queryNumber)

			//Validate username: check if empty and is valid email
			if query.Username == "" {
				return config, errors.New("error, username in configuration file ffs query: " + queryNumberString + ", is blank")
			} else {
				err = utils.ValidateUsernameRegexp(query.Username)
				if err != nil {
					return config, errors.New("error in ffs query: " + queryNumberString + ", " + err.Error())
				}
			}

			//Validate password: check if empty
			if query.Password == "" {
				return config, errors.New("error: password in configuration file ffs query: " + queryNumberString + ", is blank")
			}

			if query.QueryInterval == "" {
				return config, errors.New("error: query interval in configuration file ffs query: " + queryNumberString + ", is blank")
			} else {
				_, err := time.ParseDuration(query.QueryInterval)
				if err != nil {
					return config, errors.New("error: invalid duration provide in ffs query: " + queryNumberString)
				}
			}

			//TODO figure out how to best validate FFSQueries

			//TODO valid outputLocation
			if query.OutputLocation == "" {
				//Get working directory and set as output location
				dir, err := os.Getwd()
				if err != nil {
					return config, errors.New("error: unable to get working directory for ffs query: " + queryNumberString)
				}
				err = utils.IsWritable(dir)
				if err != nil {
					return config, err
				}
				config.FFSQueries[queryNumber].OutputLocation = dir
			} else {
				//Validate that output location is a valid path
				err = utils.IsWritable(query.OutputLocation)
				if err != nil {
					return config, err
				}
				//TODO add support for outputting to other things then just file, ie: elasticsearch
			}
		}
	}

	return config, nil
}