package main

import (
	ffs "crashplan-ffs-go-pkg"
	"crashplan-ffs-puller/config"
	"crashplan-ffs-puller/eventOutput"
	"crashplan-ffs-puller/ffsEvent"
	"errors"
	"flag"
	"github.com/google/go-cmp/cmp"
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
	for _, query := range configuration.FFSQueries {
		wg.Add(1)
		go ffsQuery(configuration, query)
	}

	wg.Wait()
}

func ffsQuery (configuration config.Config, query config.FFSQuery) {
	//Initialize query waitGroup
	var wgQuery sync.WaitGroup

	log.Println(query.Username)
	log.Println(query.Password)
	log.Println(query.QueryInterval)
	//Don't print its not formatted correctly
	//log.Println(query.Query)
	log.Println(query.OutputLocation)

	//Keep track of in progress queries by storing their ON_OR_AFTER and ON_OR_BEFORE times
	//Load saved in progress queries from last program stop
	var inProgressQueries []eventOutput.InProgressQuery
	inProgressQueries, err := eventOutput.ReadInProgressQueries(query)

	if err != nil {
		log.Println("error getting old in progress queries")
		panic(err)
	}

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
					log.Println("error with getting authentication data for ffs query: " + query.Name)
					panic(err)
				}
			}
			defer wgQuery.Done()
		}
	}()

	//Write in progress queries every 5 seconds to file
	inProgressQueryWriteInterval := 5 * time.Second
	inProgressQueryWriteTimeTicker := time.NewTicker(inProgressQueryWriteInterval)
	go func() {
		for {
			select {
			case <- inProgressQueryWriteTimeTicker.C:
				err := eventOutput.WriteInProgressQueries(query, inProgressQueries)

				if err != nil {
					panic(err)
				}
			}
			defer wgQuery.Done()
		}
	}()

	if len(inProgressQueries) > 0 {
		//TODO handle in progress queries
	}

	queryInterval, _ := time.ParseDuration(query.QueryInterval)
	queryIntervalTimeTicker := time.NewTicker(queryInterval)
	go func() {
		for {
			select {
			case <- queryIntervalTimeTicker.C:
				//Add query interval to in progress query list
				inProgressQuery, err := getOnOrBeforeAndAfter(query)
				if err != nil {
					panic(err)
				}
				inProgressQueries = append(inProgressQueries,inProgressQuery)

				fileEvents, err := ffs.GetFileEvents(authData,configuration.FFSURI, query.Query)

				if err != nil {
					log.Println("error getting file events for ffs query: " + query.Name)
					panic(err)
				}

				//TODO this is where the data should be enriched
				var ffsEvents []ffsEvent.FFSEvent

				for _, event := range fileEvents {
					ffsEvents = append(ffsEvents,ffsEvent.FFSEvent{FileEvent: event})
				}
				log.Println("Number of events for query: " + query.Name + " - " + strconv.Itoa(len(ffsEvents)))

				//Write events
				if len(ffsEvents) > 0 {
					if query.OutputType == "file" {
						err := eventOutput.WriteEvents(ffsEvents, query)

						if err != nil {
							panic(err)
						}
					}
				}

				//Remove from in progress query slice
				tempInProgress := inProgressQueries[:0]
				for _, query := range inProgressQueries {
					if !cmp.Equal(query, inProgressQuery) {
						tempInProgress = append(tempInProgress,query)
					}
				}
				inProgressQueries = tempInProgress
			}
		}
	}()
	wgQuery.Wait()
}

func getOnOrTime(beforeAfter string, query ffs.Query) (time.Time, error){
	if beforeAfter == "before" {
		for _, group := range query.Groups {
			for _, filter := range group.Filters {
				if filter.Operator == "ON_OR_BEFORE" {
					return time.Parse(time.RFC3339Nano,filter.Value)
				}
			}
		}
	}

	if beforeAfter == "after" {
		for _, group := range query.Groups {
			for _, filter := range group.Filters {
				if filter.Operator == "ON_OR_AFTER" {
					return time.Parse(time.RFC3339Nano,filter.Value)
				}
			}
		}
	}

	return time.Time{}, nil
}

func getOnOrBeforeAndAfter(query config.FFSQuery) (eventOutput.InProgressQuery,error) {
	onOrAfter, err := getOnOrTime("after", query.Query)

	if err != nil {
		return eventOutput.InProgressQuery{}, errors.New("error parsing onOrAfter time for ffs query: " + query.Name + " " + err.Error())
	}

	onOrBefore, err := getOnOrTime("before", query.Query)

	if err != nil {
		return eventOutput.InProgressQuery{}, errors.New("error parsing onOrBefore time for ffs query: " + query.Name + " " + err.Error())
	}

	 return eventOutput.InProgressQuery{
		OnOrAfter:  onOrAfter,
		OnOrBefore: onOrBefore,
	}, nil
}