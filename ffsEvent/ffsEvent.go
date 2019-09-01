package ffsEvent

import (
	"crashplan-ffs-go-pkg"
	"crashplan-ffs-puller/config"
	"crashplan-ffs-puller/eventOutput"
	"crashplan-ffs-puller/promMetrics"
	"errors"
	"github.com/google/go-cmp/cmp"
	"log"
	"reflect"
	"strconv"
	"sync"
	"time"
)

func FFSQuery (configuration config.Config, query config.FFSQuery, wg sync.WaitGroup) {
	//Initialize query waitGroup
	var wgQuery sync.WaitGroup

	log.Println(query.Username)
	log.Println(query.Password)
	log.Println(query.Interval)
	//Don't print its not formatted correctly
	log.Println(query.OutputLocation)

	//Check if there is a "max" time and set
	var maxTime time.Time
	defaultQueryTimes, err := getOnOrBeforeAndAfter(query)

	if err != nil {
		log.Println("error getting default query times")
		panic(err)
	}

	maxTime = defaultQueryTimes.OnOrBefore

	//Keep track of in progress queries by storing their ON_OR_AFTER and ON_OR_BEFORE times
	//Load saved in progress queries from last program stop
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

	if err != nil {
		log.Println("error getting auth data for ffs query: " + query.Name)
		panic(err)
	}

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
	inProgressQueryWriteInterval := 100 * time.Millisecond
	inProgressQueryWriteTimeTicker := time.NewTicker(inProgressQueryWriteInterval)
	go func() {
		var oldInProgressQueries []eventOutput.InProgressQuery
		oldInProgressQueries = inProgressQueries
		for {
			select {
			case <- inProgressQueryWriteTimeTicker.C:
				if !reflect.DeepEqual(oldInProgressQueries,inProgressQueries) {
					promMetrics.AdjustInProgressQueries(len(inProgressQueries) - len(oldInProgressQueries))
					oldInProgressQueries = inProgressQueries
					err := eventOutput.WriteInProgressQueries(query, &inProgressQueries)

					if err != nil {
						panic(err)
					}
				}
			}
			defer wgQuery.Done()
		}
	}()

	if len(inProgressQueries) > 0 {
		go func() {
			for _, inProgressQuery := range inProgressQueries {
				query = setOnOrBeforeAndAfter(query,inProgressQuery.OnOrBefore,inProgressQuery.OnOrAfter)
				queryFetcher(query, &inProgressQueries, authData, configuration, &lastCompletedQuery, maxTime, nil, wg, wgQuery, true)
			}
		}()
	}

	//Write last completed query every 1 seconds to file
	lastCompletedQueryWriteInterval := 100 * time.Millisecond
	lastCompletedQueryWriteTimeTicker := time.NewTicker(lastCompletedQueryWriteInterval)
	go func() {
		var oldLastCompletedQuery eventOutput.InProgressQuery
		oldLastCompletedQuery = lastCompletedQuery
		for {
			select {
			case <- lastCompletedQueryWriteTimeTicker.C:
				if oldLastCompletedQuery != lastCompletedQuery {
					oldLastCompletedQuery = lastCompletedQuery
					err := eventOutput.WriteLastCompletedQuery(query, lastCompletedQuery)
					if err != nil {
						panic(err)
					}
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
				go queryFetcher(query, &inProgressQueries, authData, configuration, &lastCompletedQuery, maxTime, queryIntervalTimeTicker, wg, wgQuery, false)
			}
		}
	}()
	wgQuery.Wait()
}

func queryFetcher(query config.FFSQuery, inProgressQueries *[]eventOutput.InProgressQuery, authData ffs.AuthData, configuration config.Config, lastCompletedQuery *eventOutput.InProgressQuery, maxTime time.Time, queryIntervalTimeTicker *time.Ticker, wg sync.WaitGroup, wgQuery sync.WaitGroup, cleanUpQuery bool) {
	var done bool
	var err error
	//Increment time
	//Only if it is not a catchup query (in progress queries when the app died)
	if !cleanUpQuery {
		query, done, err = calculateTimeStamps(*inProgressQueries, *lastCompletedQuery, query, maxTime)

		if err != nil {
			panic(err)
		}

		//Stop the goroutine if the max time is past
		if done {
			wg.Done()
			wgQuery.Done()
			if queryIntervalTimeTicker != nil {
				queryIntervalTimeTicker.Stop()
			}
		}
	}

	//Add query interval to in progress query list
	inProgressQuery, err := getOnOrBeforeAndAfter(query)
	if err != nil {
		panic(err)
	}

	if !cleanUpQuery {
		*inProgressQueries = append(*inProgressQueries,inProgressQuery)
	}

	fileEvents, err := ffs.GetFileEvents(authData,configuration.FFSURI, query.Query)

	if err != nil {
		log.Println("error getting file events for ffs query: " + query.Name)
		panic(err)
	}

	//TODO this is where the data should be enriched
	var ffsEvents []eventOutput.FFSEvent

	for _, event := range fileEvents {
		ffsEvents = append(ffsEvents,eventOutput.FFSEvent{FileEvent: event})
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
		*lastCompletedQuery = inProgressQuery
	}

	//Remove from in progress query slice
	temp := *inProgressQueries
	tempInProgress := temp[:0]
	for _, query := range *inProgressQueries {
		if !cmp.Equal(query, inProgressQuery) {
			tempInProgress = append(tempInProgress,query)
		}
	}
	*inProgressQueries = tempInProgress

	promMetrics.IncrementEventsProcessed(len(ffsEvents))
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

func calculateTimeStamps(inProgressQueries []eventOutput.InProgressQuery, lastCompletedQuery eventOutput.InProgressQuery, query config.FFSQuery, maxTime time.Time) (config.FFSQuery, bool, error) {
	//Create variable which will be used to store the latest query to have run
	var lastQueryInterval eventOutput.InProgressQuery

	//Set timezone
	loc, _ := time.LoadLocation("UTC")
	timeNow := time.Now().Add(-15 * time.Minute).In(loc)

	//Get time gap as a duration
	timeGap, err := time.ParseDuration(query.TimeGap)
	if err != nil {
		return query, false, err
	}

	if len(inProgressQueries) == 0 {
		if lastCompletedQuery != (eventOutput.InProgressQuery{}) {
			lastQueryInterval = lastCompletedQuery
		} else {
			currentQuery, err := getOnOrBeforeAndAfter(query)
			if err != nil {
				return query, false, err
			}
			if currentQuery.OnOrAfter == (time.Time{}) {
				lastQueryInterval = eventOutput.InProgressQuery{
					OnOrAfter:  timeNow.Add(-timeGap),
					OnOrBefore: timeNow.Add(timeGap),
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


	//TODO implement a check for "max time"
	var done bool
	if maxTime != (time.Time{}) {
		if maxTime.Sub(newOnOrAfter) <= 0 {
			done = true
		} else if maxTime.Sub(newOnOrBefore) <= 0 {
			newOnOrBefore = maxTime
		}
	}

	//Truncate time if within the 15 minute no go window
	if timeNow.Sub(newOnOrBefore) <= 0 {
		newOnOrBefore = timeNow
	}

	//Increment time
	return setOnOrBeforeAndAfter(query,newOnOrBefore,newOnOrAfter), done, nil
}

func getNewerTimeQuery(lastInProgressQuery eventOutput.InProgressQuery, lastCompletedQuery eventOutput.InProgressQuery) eventOutput.InProgressQuery {
	if lastCompletedQuery.OnOrBefore.Sub(lastInProgressQuery.OnOrAfter) <= 0 {
		return lastInProgressQuery
	} else {
		return lastCompletedQuery
	}
}