package config

import (
	"encoding/json"
	"errors"
	"github.com/BenB196/crashplan-ffs-go-pkg"
	"github.com/BenB196/crashplan-ffs-puller/utils"
	"github.com/BenB196/ip-api-go-pkg"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

//Configuration File structs
type Config struct {
	AuthURI  		string 			`json:"authURI"`
	FFSURI   		string 			`json:"ffsURI"`
	FFSQueries		[]FFSQuery		`json:"ffsQueries"`
	Prometheus		Prometheus		`json:"prometheus,omitempty"`
}

type FFSQuery struct {
	Name			string			`json:"name"`
	Username 		string			`json:"username"`
	Password 		string 			`json:"password"`
	Interval		string			`json:"interval"`
	TimeGap			string			`json:"timeGap"`
	Query			ffs.Query 		`json:"query"`
	OutputType		string			`json:"outputType"`
	OutputLocation  string			`json:"outputLocation,omitempty"`
	IPAPI			IPAPI			`json:"ip-api,omitempty"`
	Elasticsearch	Elasticsearch	`json:"elasticsearch,omitempty"`
	Logstash		Logstash		`json:"logstash,omitempty"`
}

type IPAPI struct {
	Enabled			bool			`json:"enabled,omitempty"`
	URL				string			`json:"url,omitempty"`
	APIKey			string			`json:"apiKey,omitempty"`
	Fields			string			`json:"fields,omitempty"`
	Lang			string			`json:"lang,omitempty"`
}

type Elasticsearch struct {
	NumberOfShards			int			`json:"numberOfShards,omitempty"`
	NumberOfReplicas		int			`json:"numberOfReplicas,omitempty"`
	IndexName				string		`json:"indexName,omitempty"`
	IndexTimeAppend			string		`json:"indexTimeAppend,omitempty"`
	IndexTimeGen			string		`json:"indexTimeGen,omitempty"`
	ElasticURL				string		`json:"elasticUrl,omitempty"`
	UseCustomIndexPattern	bool		`json:"useCustomIndexPattern"`
	BasicAuth				BasicAuth	`json:"basicAuth,omitempty"`
	Sniffing				bool		`json:"sniffing,omitempty"`
	BestCompression			bool		`json:"bestCompression,omitempty"`
	RefreshInterval			int			`json:"refreshInterval,omitempty"`
	Aliases					[]string	`json:"aliases,omitempty"`
	EsStandardized			bool		`json:"esStandardized,omitempty"`
}

type Logstash struct {
	LogstashURL string 	`json:"logstashURL"`
}

type BasicAuth struct {
	User			string			`json:"user,omitempty"`
	Password		string			`json:"password,omitempty"`
}

type Prometheus struct {
	Enabled			bool			`json:"enabled,omitempty"`
	Port			int				`json:"port,omitempty"`
}

/*
ReadConfig - read a configuration file from a specified location
configLocation - string which contains the location of the configuration file
Returns
Config - config struct of the configuration file (can be a "new" struct as well)
error - any errors which have been caught
 */
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
	fileExtension = strings.ToLower(fileExtension)

	//If no file extension err
	switch fileExtension {
	case "":
		return Config{}, errors.New("no file extension found on configuration file, unable to properly parse")
	case ".json":
		return validateConfigJson(byteValue)
	case ".yaml",".yml":
		//TODO parse file as yaml
		return Config{}, nil
	default:
		return Config{}, errors.New("unknown file extension: " + fileExtension + ", supported file extensions: json, yaml, yml")
	}
}

/*
validateConfigJson - validates the configuration file bytes to make sure the configuration file is accurate
fileBytes - the bytes of the configuration file
Returns
Config - config struct of the configuration file (can be a "new" struct as well)
error - any errors which have been caught
 */
func validateConfigJson(fileBytes []byte) (Config, error) {
	//create config struct
	var config Config
	//Make sure file is valid JSON
	err := json.Unmarshal(fileBytes, &config)

	//return error if unmarshal fails
	if err != nil {
		return config, errors.New("error parsing JSON configuration file: " + err.Error())
	}

	//Validate JSON fields
	//Validate AuthURI
	//check if empty
	if config.AuthURI == "" {
		return config, errors.New("error: authURI cannot be blank")
	} else {
		//check if valid URI
		_, err := url.ParseRequestURI(config.AuthURI)
		if err != nil {
			return config, errors.New("error: bad authURI provided: " + err.Error())
		}
	}

	//Validate FFSURI
	//check if empty
	if config.FFSURI == "" {
		return config, errors.New("error: FFSURI cannot be blank")
	} else {
		//check if valid URI
		_, err := url.ParseRequestURI(config.FFSURI)
		if err != nil {
			return config, errors.New("error: bad FFSURI provided: " + err.Error())
		}
	}

	//Validate prometheus port
	if config.Prometheus.Enabled {
		//Set default port to 8080 if port == 0
		if config.Prometheus.Port == 0 {
			config.Prometheus.Port = 8080
		} else if config.Prometheus.Port < 1024 {
			return config, errors.New("error: prometheus port is below 1024, port must be between 1024 and 65535")
		} else if config.Prometheus.Port > 65535 {
			return config, errors.New("error: prometheus port is above 65535, port must be between 1024 and 65535")
		}
	}

	//Create queryName slice
	var queryNames []string

	//Validate FFS Queries
	if config.FFSQueries == nil {
		return config, errors.New("error: no ffs queries provided")
	} else {
		for i, query := range config.FFSQueries {

			//Validate query.Name
			//check if empty
			if query.Name == "" {
				return config, errors.New("error: query name is empty")
			} else if len(query.Name) > 100 {
				//check if greater than 100 characters
				return config, errors.New("error: query name: " + query.Name + ", is greater than 100 characters")
			} else {
				//Check if query name is unique
				if len(queryNames) > 0 {
					for _, name := range queryNames {
						if name == query.Name {
							return config, errors.New("error: duplicate query names provided, query names must be unique")
						}
					}
				} else {
					queryNames = append(queryNames, query.Name)
				}
			}

			//Validate username
			//check if empty
			if query.Username == "" {
				return config, errors.New("error: username in ffs query: " + query.Name + ", is blank")
			} else {
				//check if valid email address
				err = utils.ValidateUsernameRegexp(query.Username)
				if err != nil {
					return config, errors.New("error: in ffs query: " + query.Name + ", " + err.Error())
				}
			}

			//Validate password
			//check if empty
			if query.Password == "" {
				return config, errors.New("error: password in ffs query: " + query.Name + ", is blank")
			}

			//Validate interval
			//check if empty
			if query.Interval == "" {
				return config, errors.New("error: interval in ffs query: " + query.Name + ", is blank")
			} else {
				//check if real duration value is passed
				_, err := time.ParseDuration(query.Interval)
				if err != nil {
					return config, errors.New("error: invalid duration provide in ffs query for interval: " + query.Name)
				}
			}

			//Validate time gap
			//check if empty
			if query.TimeGap == "" {
				return config, errors.New("error: time gap in ffs query: " + query.Name + ", is blank")
			} else {
				//check if real duration value is passed
				_, err := time.ParseDuration(query.TimeGap)
				if err != nil {
					return config, errors.New("error: invalid duration provide in ffs query for time gap: " + query.Name)
				}
			}

			//TODO figure out how to best validate FFSQueries
			//TODO need to validate that both ON_OR_AFTER and ON_OR_BEFORE exist once

			//Validate Output Type
			//check if empty
			if query.OutputType == "" {
				return config, errors.New("error: output type ")
			} else {
				switch query.OutputType {
				case "file":
					//Validate Output Location
					if query.OutputLocation == "" {
						//Get working directory and set as output location
						dir, err := os.Getwd()
						//return any errors
						if err != nil {
							return config, errors.New("error: unable to get working directory for ffs query: " + query.Name)
						}
						//check if directory is writable
						err = utils.IsWritable(dir)
						//return any errors
						if err != nil {
							return config, err
						}
						//update output location to absolute path
						config.FFSQueries[i].OutputLocation = dir + utils.DirPath
					} else {
						//Validate that output location is a valid path
						//check that path is writable
						err = utils.IsWritable(query.OutputLocation)
						//return any errors
						if err != nil {
							return config, err
						}

						//Append a / or \\ to end of path if not there
						lastChar := query.OutputLocation[len(query.OutputLocation)-1:]
						if lastChar != utils.DirPath {
							config.FFSQueries[i].OutputLocation = query.OutputLocation + utils.DirPath
						}
					}
				case "elastic":
					//Validate output location, this is still needed for writing files to keep track on in progress and last completed queries
					if query.OutputLocation == "" {
						//Get working directory and set as output location
						dir, err := os.Getwd()
						//return any errors
						if err != nil {
							return config, errors.New("error: unable to get working directory for ffs query: " + query.Name)
						}
						//check if directory is writable
						err = utils.IsWritable(dir)
						//return any errors
						if err != nil {
							return config, err
						}
						//update output location to absolute path
						config.FFSQueries[i].OutputLocation = dir + utils.DirPath
					} else {
						//Validate that output location is a valid path
						//check that path is writable
						err = utils.IsWritable(query.OutputLocation)
						//return any errors
						if err != nil {
							return config, err
						}

						//Append a / or \\ to end of path if not there
						lastChar := query.OutputLocation[len(query.OutputLocation)-1:]
						if lastChar != utils.DirPath {
							config.FFSQueries[i].OutputLocation = query.OutputLocation + utils.DirPath
						}
					}

					//validate number of shards
					if !query.Elasticsearch.UseCustomIndexPattern && query.Elasticsearch.NumberOfShards < 1 {
						return config, errors.New("error: number of shards for ffs query: " + query.Name + " cannot be lower than 1")
					}

					//validate number of replicas
					if !query.Elasticsearch.UseCustomIndexPattern && query.Elasticsearch.NumberOfReplicas < 0 {
						return config, errors.New("error: number of shards for ffs query: " + query.Name + " cannot be lower than 0")
					}

					//validate index name
					err = utils.ValidateIndexName(query.Elasticsearch.IndexName)

					if err != nil {
						return config, errors.New("error: in ffs query: " + query.Name + " : " + err.Error())
					}

					//check if indexTimeAppend is set, validate and get length, will need to add to length of index name and validate not > 255 characters
					if query.Elasticsearch.IndexTimeAppend != "" {
						//TODO figure out a way to validate golang time format
						if len(query.Elasticsearch.IndexTimeAppend) + len(query.Elasticsearch.IndexName) + 1 > 255 {
							return config, errors.New("error: index name cannot be longer than 255 characters")
						}
					} else {
						config.FFSQueries[i].Elasticsearch.IndexTimeAppend = "2006-01-02"
					}

					//validate indexTimeGen, must either be timeNow, or onOrBefore
					if query.Elasticsearch.IndexTimeGen == "" {
						config.FFSQueries[i].Elasticsearch.IndexTimeGen = "timeNow"
					} else if query.Elasticsearch.IndexTimeGen != "timeNow" && query.Elasticsearch.IndexTimeGen != "onOrBefore" && query.Elasticsearch.IndexTimeGen != "eventTimestamp" && query.Elasticsearch.IndexTimeGen != "insertionTimestamp" {
						return config, errors.New("error: elasticsearch indexTimeGen must be timeNow, onOrBefore, eventTimestamp, or insertionTimestamp")
					}

					//Validate elasticUrl
					//check if empty
					if query.Elasticsearch.ElasticURL == "" {
						return config, errors.New("error: elastic url cannot be blank")
					} else {
						//check if valid URI
						_, err := url.ParseRequestURI(query.Elasticsearch.ElasticURL)
						if err != nil {
							return config, errors.New("error: invalid elastic url provided: " + err.Error())
						}
					}

					//validate aliases
					if !query.Elasticsearch.UseCustomIndexPattern && len(query.Elasticsearch.Aliases) > 0 {
						for _, alias := range query.Elasticsearch.Aliases {
							//validate alias names
							err = utils.ValidateIndexName(alias)

							if err != nil {
								return config, errors.New("error: in ffs query: " + query.Name + " : " + err.Error())
							}
						}
					}
				case "logstash":
					//Validate output location, this is still needed for writing files to keep track on in progress and last completed queries
					if query.OutputLocation == "" {
						//Get working directory and set as output location
						dir, err := os.Getwd()
						//return any errors
						if err != nil {
							return config, errors.New("error: unable to get working directory for ffs query: " + query.Name)
						}
						//check if directory is writable
						err = utils.IsWritable(dir)
						//return any errors
						if err != nil {
							return config, err
						}
						//update output location to absolute path
						config.FFSQueries[i].OutputLocation = dir + utils.DirPath
					} else {
						//Validate that output location is a valid path
						//check that path is writable
						err = utils.IsWritable(query.OutputLocation)
						//return any errors
						if err != nil {
							return config, err
						}

						//Append a / or \\ to end of path if not there
						lastChar := query.OutputLocation[len(query.OutputLocation)-1:]
						if lastChar != utils.DirPath {
							config.FFSQueries[i].OutputLocation = query.OutputLocation + utils.DirPath
						}
					}

					if query.Logstash.LogstashURL == "" {
						return config, errors.New("error: logstash url cannot be blank")
					}
				default:
					return config, errors.New("unknown output type provide in ffs query: " + query.Name + ", output type provided: " + query.OutputType)
				}
			}

			//Validate ip-api
			if query.IPAPI != (IPAPI{}) && query.IPAPI.Enabled {

				//validate URL is valid if provided
				if query.IPAPI.URL != "" {
					_, err := url.ParseRequestURI(query.IPAPI.URL)
					if err != nil {
						return config, errors.New("error: bad ip api URL provided: " + err.Error())
					}
				}

				//validate fields
				if query.IPAPI.Fields != "" {
					_, err = ip_api.ValidateFields(query.IPAPI.Fields)

					if err != nil {
						return config, err
					}

					//Make sure that this field is passed as it needs to used. Will be dropped anyway
					if !strings.Contains(query.IPAPI.Fields,"query") {
						config.FFSQueries[i].IPAPI.Fields = query.IPAPI.Fields + ",query"
					}
				}

				//validate lang
				if query.IPAPI.Lang != "" {
					_, err = ip_api.ValidateLang(query.IPAPI.Lang)

					if err != nil {
						return config, err
					}
				}
			}
		}
	}

	return config, nil
}