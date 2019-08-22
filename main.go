package main

import (
	ffs "crashplan-ffs-go-pkg"
	"crashplan-ffs-puller/config"
	"flag"
	"log"
	"strconv"
	"sync"
	"time"
)

func main() {

	//Get Config location and get config struct
	var configLocation string
	flag.StringVar(&configLocation,"config","","Configuation File Location. REQUIRED") //TODO improve usage description


	//Parse Flags
	flag.Parse()

	//Panic and die if configLocation not set
	if configLocation == "" {
		panic("config flag missing, required.")
	}

	//Get config struct
	configuration, err := config.ReadConfig(configLocation)

	if err != nil {
		log.Println("Error parsing config file.")
		panic(err)
	}

	var wg sync.WaitGroup

	//Quick lazy test
	log.Println(configuration.AuthURI)
	log.Println(configuration.FFSURI)
	//TODO this should spawn go routines?
	for queryNumber, query := range configuration.FFSQueries {
		log.Println(query.Username)
		log.Println(query.Password)
		log.Println(query.QueryInterval)
		log.Println(query.Query)

		//Handle getting API AuthTokens every 55 minutes
		apiTokenRefreshInterval := 55 * time.Minute
		authTimeTicker := time.NewTicker(apiTokenRefreshInterval)

		//Get initial authData
		authData, err := ffs.GetAuthData(configuration.AuthURI,query.Username,query.Password)
		wg.Add(1)
		go func() {
			for {
				select {
				case <- authTimeTicker.C:
					authData, err = ffs.GetAuthData(configuration.AuthURI,query.Username,query.Password)
				}
				defer wg.Done()
			}
		}()

		if err != nil {
			log.Println("error with getting authentication data for ffs query: " + strconv.Itoa(queryNumber))
			panic(err)
		}

		for {
			log.Println(authData.Data.V3UserToken)
			time.Sleep(apiTokenRefreshInterval)
		}
	}

}