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
	log.Println(query.Interval)
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

	//Keep track of the last successfully completed query
	var lastCompletedQuery eventOutput.InProgressQuery
	lastCompletedQuery, err = eventOutput.ReadLastCompletedQuery(query)

	if err != nil {
		log.Println("error getting old last completed query")
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

	//Write in progress queries every 1 seconds to file
	inProgressQueryWriteInterval := 1 * time.Second
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
		go func() {
			for _, inProgressQuery := range inProgressQueries {
				query = setOnOrBeforeAndAfter(query,inProgressQuery.OnOrBefore,inProgressQuery.OnOrAfter)
				lastCompletedQuery, inProgressQueries = queryFetcher(query, inProgressQueries, authData, configuration, lastCompletedQuery)
			}
		}()
	}

	//Write last completed query every 1 seconds to file
	lastCompletedQueryWriteInterval := 1 * time.Second
	lastCompletedQueryWriteTimeTicker := time.NewTicker(lastCompletedQueryWriteInterval)
	go func() {
		for {
			select {
			case <- lastCompletedQueryWriteTimeTicker.C:
				err := eventOutput.WriteLastCompletedQuery(query, lastCompletedQuery)
				if err != nil {
					panic(err)
				}
			}
			defer wgQuery.Done()
		}
	}()

	//Handle setting the initial ON_OR_BEFORE and ON_OR_AFTER depending on the saved lastCompletedQuery
	if lastCompletedQuery != (eventOutput.InProgressQuery{}) {
		//TODO handle setting correct times
	}

	queryInterval, _ := time.ParseDuration(query.Interval)
	queryIntervalTimeTicker := time.NewTicker(queryInterval)
	go func() {
		for {
			select {
			case <- queryIntervalTimeTicker.C:
				lastCompletedQuery, inProgressQueries = queryFetcher(query, inProgressQueries, authData, configuration, lastCompletedQuery)
			}
		}
	}()
	wgQuery.Wait()
}

func queryFetcher(query config.FFSQuery, inProgressQueries []eventOutput.InProgressQuery, authData ffs.AuthData, configuration config.Config, lastCompletedQuery eventOutput.InProgressQuery) (eventOutput.InProgressQuery, []eventOutput.InProgressQuery) {
	//Increment time
	query, err := calculateTimeStamps(inProgressQueries, lastCompletedQuery, query)

	if err != nil {
		panic(err)
	}

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

	//Check if this query is the newest completed query, if it is, set last completed query to query times
	if lastCompletedQuery.OnOrBefore.Sub(inProgressQuery.OnOrAfter) <= 0 {
		lastCompletedQuery = inProgressQuery
	}

	//Remove from in progress query slice
	tempInProgress := inProgressQueries[:0]
	for _, query := range inProgressQueries {
		if !cmp.Equal(query, inProgressQuery) {
			tempInProgress = append(tempInProgress,query)
		}
	}
	inProgressQueries = tempInProgress

	return lastCompletedQuery, inProgressQueries
}

func getOnOrTime(beforeAfter string, query ffs.Query) (time.Time, error){
	for _, group := range query.Groups {
		for _, filter := range group.Filters {
			if beforeAfter == "before" && filter.Operator == "ON_OR_BEFORE" {
				if filter.Value == "" || filter.Value == (time.Time{}.String()) {
					return time.Time{}, nil
				} else {
					return time.Parse(time.RFC3339Nano,filter.Value)
				}
			} else if beforeAfter == "after" && filter.Operator == "ON_OR_AFTER" {
				if filter.Value == "" || filter.Value == (time.Time{}.String()) {
					return time.Time{}, nil
				} else {
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

func setOnOrTime(beforeAfter string, query ffs.Query, timeStamp time.Time) ffs.Query {
	for k, group := range query.Groups {
		for i, filter := range group.Filters {
			if beforeAfter == "before" && filter.Operator == "ON_OR_BEFORE" {
				query.Groups[k].Filters[i].Value = timeStamp.Format("2006-01-02T15:04:05.000Z")
			} else if beforeAfter == "after" && filter.Operator == "ON_OR_AFTER" {
				query.Groups[k].Filters[i].Value = timeStamp.Format("2006-01-02T15:04:05.000Z")
			}
		}
	}

	return query
}

func setOnOrBeforeAndAfter(query config.FFSQuery, beforeTime time.Time, afterTime time.Time) config.FFSQuery {
	query.Query = setOnOrTime("before", query.Query, beforeTime)
	query.Query = setOnOrTime("after", query.Query, afterTime)

	return query
}

	//Logic for setting the correct times
	//TODO make sure on or before never exceeds time.Now -15 minutes. This is what Code42 sets as expected time for logs to be ready for pulling
	//If len(inProgressQueries) == 0
		//then check last completed query
		//If last completed query is "empty"
			//then get ffs query times
			//If ffs query on or after is empty
				//then set to time.now
				//else do nothing
			//If ffs query on or before is empty
				//then set to on or after + query time Interval
				//else this is max time and should not be exceeded TODO save this to a "max time variable" that is checked only on program startup
		//else set time based off of last completed query + time gap
	//else get last inProgressQuery
	//then check if last completed query is set
		//if last completed query is set
		//then compare last in progress query to last completed query and see which is newer
		//else set time based off of last in progress query + time gap

func calculateTimeStamps(inProgressQueries []eventOutput.InProgressQuery, lastCompletedQuery eventOutput.InProgressQuery, query config.FFSQuery) (config.FFSQuery, error) {
	//Create variable which will be used to store the latest query to have run
	var lastQueryInterval eventOutput.InProgressQuery

	//Get time gap as a duration
	timeGap, err := time.ParseDuration(query.TimeGap)
	if err != nil {
		return query, err
	}

	if len(inProgressQueries) == 0 {
		if lastCompletedQuery != (eventOutput.InProgressQuery{}) {
			lastQueryInterval = lastCompletedQuery
		} else {
			currentQuery, err := getOnOrBeforeAndAfter(query)
			if err != nil {
				return query, err
			}
			if currentQuery.OnOrAfter == (time.Time{}) {
				lastQueryInterval = eventOutput.InProgressQuery{
					OnOrAfter:  time.Now().Add(-15 * time.Minute).Add(-timeGap),
					OnOrBefore: time.Now().Add(-15 * time.Minute).Add(timeGap),
				}
			} else {
				lastQueryInterval = eventOutput.InProgressQuery{
					OnOrAfter:  currentQuery.OnOrAfter.Add(1 * time.Millisecond),
					OnOrBefore: currentQuery.OnOrAfter.Add(1 * time.Millisecond).Add(timeGap),
				}
			}
		}
	} else {
		lastInProgressQuery := inProgressQueries[len(inProgressQueries) - 1]
		if lastCompletedQuery != (eventOutput.InProgressQuery{}) {
			lastQueryInterval = getNewerTimeQuery(lastInProgressQuery, lastCompletedQuery)
		} else {
			lastQueryInterval = lastInProgressQuery
		}
	}

	//set time variables
	newOnOrAfter := lastQueryInterval.OnOrBefore.Add(1 * time.Millisecond)
	newOnOrBefore := lastQueryInterval.OnOrBefore.Add(1 * time.Millisecond).Add(timeGap)
	timeNow := time.Now().Add(-15 * time.Minute)

	//TODO implement a check for "max time"

	//Truncate time if within the 15 minute no go window
	if timeNow.Sub(newOnOrBefore) <= 0 {
		newOnOrBefore = timeNow
	}

	//Increment time
	return setOnOrBeforeAndAfter(query,newOnOrBefore,newOnOrAfter), nil
}

func getNewerTimeQuery(lastInProgressQuery eventOutput.InProgressQuery, lastCompletedQuery eventOutput.InProgressQuery) eventOutput.InProgressQuery {
	if lastCompletedQuery.OnOrBefore.Sub(lastInProgressQuery.OnOrAfter) <= 0 {
		return lastInProgressQuery
	} else {
		return lastCompletedQuery
	}
}