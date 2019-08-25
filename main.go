package main

import (
	ffs "crashplan-ffs-go-pkg"
	"crashplan-ffs-puller/config"
	"crashplan-ffs-puller/eventOutput"
	"crashplan-ffs-puller/ffsEvent"
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

	//Print Auth and FFS URIs for debugging
	log.Println(configuration.AuthURI)
	log.Println(configuration.FFSURI)

	//Spawn goroutines for each ffs query provided
	var wg sync.WaitGroup
	for queryNumber, query := range configuration.FFSQueries {
		wg.Add(1)
		go ffsQuery(configuration, queryNumber, query)
	}

	wg.Wait()
}

func ffsQuery (configuration config.Config,queryNumber int, query config.FFSQuery) {
	//Initialize query waitGroup
	var wgQuery sync.WaitGroup

	log.Println(query.Username)
	log.Println(query.Password)
	log.Println(query.QueryInterval)
	//Don't print its not formatted correctly
	//log.Println(query.Query)
	log.Println(query.OutputLocation)

	//Handle getting API AuthTokens every 55 minutes
	apiTokenRefreshInterval := 55 * time.Minute
	authTimeTicker := time.NewTicker(apiTokenRefreshInterval)

	//Get initial authData
	authData, err := ffs.GetAuthData(configuration.AuthURI,query.Username,query.Password)
	//Init goroutine for getting authData ever 55 minutes
	wgQuery.Add(1)
	go func() {
		for {
			select {
			case <- authTimeTicker.C:
				authData, err = ffs.GetAuthData(configuration.AuthURI,query.Username,query.Password)

				if err != nil {
					log.Println("error with getting authentication data for ffs query: " + strconv.Itoa(queryNumber))
					panic(err)
				}
			}
			defer wgQuery.Done()
		}
	}()

	queryInterval, _ := time.ParseDuration(query.QueryInterval)
	queryIntervalTimeTicker := time.NewTicker(queryInterval)
	go func() {
		for {
			select {
			case <- queryIntervalTimeTicker.C:
				fileEvents, err := ffs.GetFileEvents(authData,configuration.FFSURI, query.Query)

				if err != nil {
					log.Println("error getting file events for ffs query: " + strconv.Itoa(queryNumber))
					panic(err)
				}

				//TODO this is where the data should be enriched
				var ffsEvents []ffsEvent.FFSEvent

				for _, event := range fileEvents {
					ffsEvents = append(ffsEvents,ffsEvent.FFSEvent{FileEvent: event})
				}
				log.Println("Number of events: " + strconv.Itoa(len(ffsEvents)))

				//Write events
				if len(ffsEvents) > 0 {
					err := eventOutput.WriteEvents(ffsEvents, query.OutputLocation, query.Query)

					if err != nil {
						panic(err)
					}
				}
			}
		}
	}()

	wgQuery.Wait()
}