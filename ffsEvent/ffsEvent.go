package ffsEvent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/BenB196/crashplan-ffs-go-pkg"
	"github.com/BenB196/crashplan-ffs-puller/config"
	"github.com/BenB196/crashplan-ffs-puller/elasticsearch"
	"github.com/BenB196/crashplan-ffs-puller/eventOutput"
	"github.com/BenB196/crashplan-ffs-puller/promMetrics"
	"github.com/BenB196/ip-api-go-pkg"
	"github.com/google/go-cmp/cmp"
	"github.com/olivere/elastic/v7"
	"log"
	"reflect"
	"strconv"
	"sync"
	"time"
)

func FFSQuery (configuration config.Config, query config.FFSQuery) {
	//Initialize query waitGroup
	var wgQuery sync.WaitGroup

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
	authTimeTicker := time.NewTicker(55 * time.Minute)

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

	//Write in progress queries every 100 milliseconds to file
	inProgressQueryWriteTimeTicker := time.NewTicker(100 * time.Millisecond)
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

	//Init elastic client if output type == elastic
	var elasticClient *elastic.Client
	var ctx context.Context

	if query.OutputType == "elastic" {
		//Create context
		ctx = context.Background()

		//Create elastic client
		elasticClient, err = elasticsearch.BuildElasticClient(query.Elasticsearch)

		if err != nil {
			//TODO handle error
			log.Println("error building elastic client")
			panic(err)
		}

		//get elastic info
		info, code, err := elasticClient.Ping(query.Elasticsearch.ElasticURL).Do(ctx)

		if err != nil {
			//TODO handle error
			log.Println("error reaching elastic server")
			panic(err)
		}

		fmt.Printf("Elasticsearch returned with code %d and version %s\n", code, info.Version.Number)
	}

	//Handle old in progress queries that never completed when programmed died
	if len(inProgressQueries) > 0 {
		go func() {
			for _, inProgressQuery := range inProgressQueries {
				query = setOnOrBeforeAndAfter(query,inProgressQuery.OnOrBefore,inProgressQuery.OnOrAfter)
				queryFetcher(query, &inProgressQueries, authData, configuration, &lastCompletedQuery, maxTime, nil, true, elasticClient, ctx)
			}
		}()
	}

	//Write last completed query every 100 milliseconds to file
	lastCompletedQueryWriteTimeTicker := time.NewTicker(100 * time.Millisecond)
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
				go queryFetcher(query, &inProgressQueries, authData, configuration, &lastCompletedQuery, maxTime, queryIntervalTimeTicker,false, elasticClient, ctx)
			}
			defer wgQuery.Done()
		}
	}()
	wgQuery.Wait()
}

func queryFetcher(query config.FFSQuery, inProgressQueries *[]eventOutput.InProgressQuery, authData ffs.AuthData, configuration config.Config, lastCompletedQuery *eventOutput.InProgressQuery, maxTime time.Time, queryIntervalTimeTicker *time.Ticker, cleanUpQuery bool, client *elastic.Client, ctx context.Context) {
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
			if queryIntervalTimeTicker != nil {
				queryIntervalTimeTicker.Stop()
			}
			return
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

	//Write events
	var ffsEvents []eventOutput.FFSEvent

	if len(fileEvents) > 0 {
		//TODO this is where the data should be enriched
		//Add ip-api data if enabled
		var locationMap = map[string]ip_api.Location{}
		if query.IPAPI.Enabled {
			//Build ip api query
			//init vars
			var ipApiQuery ip_api.Query
			ipApiQuery.Fields = query.IPAPI.Fields
			ipApiQuery.Lang = query.IPAPI.Lang
			var queryMap = map[string]interface{}{}
			var queryIPs []ip_api.QueryIP
			var locations []ip_api.Location
			var ipApiWg sync.WaitGroup

			//Loop through events and build batch query
			ipApiWg.Add(len(fileEvents))
			for _, event := range fileEvents {
				if event.PublicIpAddress != "" {
					//if queryIPs is not new
					if len(queryIPs) > 0 {
						//check to make sure map does not already contain ip, don't want to query same IP multiple times
						if _, ok := queryMap[event.PublicIpAddress]; !ok {
							queryIPs = append(queryIPs,ip_api.QueryIP{
								Query:  event.PublicIpAddress,
							})
							queryMap[event.PublicIpAddress] = nil
						}
					} else {
						queryIPs = append(queryIPs,ip_api.QueryIP{
							Query:  event.PublicIpAddress,
						})
						queryMap[event.PublicIpAddress] = nil
					}
				}
				ipApiWg.Done()
			}

			ipApiWg.Wait()

			ipApiQuery.Queries = queryIPs

			locations, err = ip_api.BatchQuery(ipApiQuery,query.IPAPI.APIKey,query.IPAPI.URL)

			if err != nil {
				log.Println(err)
			}

			//Move slice to map for easy lookups
			for _, location := range locations {
				locationMap[location.Query] = location
			}
		}

		var eventWg sync.WaitGroup
		eventWg.Add(len(fileEvents))
		go func() {
			for _, event := range fileEvents {
				if event.PublicIpAddress != "" {
					if len(locationMap) == 0 {
						ffsEvents = append(ffsEvents,eventOutput.FFSEvent{FileEvent: event})
					} else if location, ok := locationMap[event.PublicIpAddress]; ok {
						//nil this as it is not needed, we already have event.publicIpAddress
						location.Query = ""
						geoPoint := eventOutput.GeoPoint{
							Lat: location.Lat,
							Lon: location.Lon,
						}
						ffsEvents = append(ffsEvents,eventOutput.FFSEvent{FileEvent: event, Location: location, GeoPoint: &geoPoint})
					} else {
						b, _ := json.Marshal(event)
						log.Println("error getting location for fileEvent: " + string(b))
						panic("Unable to find location which should exist.")
					}
				}
				defer eventWg.Done()
			}
		}()

		eventWg.Wait()
		log.Println("Number of events for query: " + query.Name + " - " + strconv.Itoa(len(ffsEvents)))

		switch query.OutputType {
		case "file":
			err := eventOutput.WriteEvents(ffsEvents, query)

			if err != nil {
				panic(err)
			}
		case "elastic":
			//setup bulk processor
			var processor *elastic.BulkProcessor
			processor, err = client.BulkProcessor().Name(query.Name + "BGWorker").Workers(2).Do(ctx)

			var elasticWg sync.WaitGroup

			//get index name based off of query end time
			if query.Elasticsearch.IndexTimeGen == "timeNow" || query.Elasticsearch.IndexTimeGen == "onOrBefore" {
				var indexName string
				if query.Elasticsearch.IndexTimeGen == "timeNow" {
					indexName = elasticsearch.BuildIndexName(query.Elasticsearch)
				} else {
					indexName = elasticsearch.BuildIndexNameWithTime(query.Elasticsearch,inProgressQuery.OnOrBefore)
				}

				//check if index exists if not create
				exists, err := client.IndexExists(indexName).Do(ctx)

				if err != nil {
					//TODO handle err
					log.Println("error checking if elastic index exists: " + indexName)
					panic(err)
				}

				if !exists {
					//create index
					createIndex, err := client.CreateIndex(indexName).BodyString(elasticsearch.BuildIndexPattern(query.Elasticsearch)).Do(ctx)

					if err != nil {
						//TODO handle err
						log.Println("error creating elastic index: " + indexName)
						log.Println(elasticsearch.BuildIndexPattern(query.Elasticsearch))
						panic(err)
					}

					if !createIndex.Acknowledged {
						//TODO handle the creation not being acknowledged
						panic("elasticsearch index creation failed for: " + indexName)
					}
				}
				elasticWg.Add(len(ffsEvents))
				go func() {
					for _, ffsEvent := range ffsEvents {
						r := elastic.NewBulkIndexRequest().Index(indexName).Doc(ffsEvent)
						processor.Add(r)
						elasticWg.Done()
					}
				}()
				elasticWg.Wait()

				err = processor.Flush()

				if err != nil {
					//TODO handle err
					log.Println("error flushing elastic bulk request")
					panic(err)
				}
			} else {
				//create map of indexes required
				var requiredIndexTimestamps = map[time.Time]interface{}{}
				var requiredIndexMutex = sync.RWMutex{}
				elasticWg.Add(len(ffsEvents))
				go func() {
					for _, ffsEvent :=range ffsEvents {
						var indexTime time.Time
						if query.Elasticsearch.IndexTimeGen == "insertionTimestamp" {
							indexTime, _ = time.Parse(query.Elasticsearch.IndexTimeAppend,ffsEvent.InsertionTimestamp.String())
						} else {
							indexTime, _ = time.Parse(query.Elasticsearch.IndexTimeAppend,ffsEvent.EventTimestamp.String())
						}
						requiredIndexMutex.RLock()
						if _, found := requiredIndexTimestamps[indexTime]; !found {
							requiredIndexTimestamps[indexTime] = nil
						}
						requiredIndexMutex.RUnlock()
						elasticWg.Done()
					}
				}()
				elasticWg.Wait()

				//check if indexes exist
				elasticWg.Add(len(requiredIndexTimestamps))
				go func() {
					for timestamp, _ := range requiredIndexTimestamps {
						//generate indexName
						indexName := elasticsearch.BuildIndexNameWithTime(query.Elasticsearch,timestamp)
						exists, err := client.IndexExists(indexName).Do(ctx)

						if err != nil {
							//TODO handle err
							log.Println("error checking if elastic index exists: " + indexName)
							panic(err)
						}

						if !exists {
							//create index
							createIndex, err := client.CreateIndex(indexName).BodyString(elasticsearch.BuildIndexPattern(query.Elasticsearch)).Do(ctx)

							if err != nil {
								//TODO handle err
								log.Println("error creating elastic index: " + indexName)
								log.Println(elasticsearch.BuildIndexPattern(query.Elasticsearch))
								panic(err)
							}

							if !createIndex.Acknowledged {
								//TODO handle the creation not being acknowledged
								panic("elasticsearch index creation failed for: " + indexName)
							}
						}
						elasticWg.Done()
					}
				}()

				elasticWg.Wait()

				//build bulk request
				elasticWg.Add(len(ffsEvents))
				go func() {
					for _, ffsEvent := range ffsEvents {
						var indexTime time.Time
						if query.Elasticsearch.IndexTimeGen == "insertionTimestamp" {
							indexTime, _ = time.Parse(ffsEvent.InsertionTimestamp.String(),query.Elasticsearch.IndexTimeAppend)
						} else {
							indexTime, _ = time.Parse(ffsEvent.EventTimestamp.String(),query.Elasticsearch.IndexTimeAppend)
						}
						indexName := elasticsearch.BuildIndexNameWithTime(query.Elasticsearch,indexTime)
						r := elastic.NewBulkIndexRequest().Index(indexName).Doc(ffsEvent)
						processor.Add(r)
						elasticWg.Done()
					}
				}()
				elasticWg.Wait()

				err = processor.Flush()

				if err != nil {
					//TODO handle err
					log.Println("error flushing elastic bulk request")
					panic(err)
				}
			}

			err = processor.Close()

			if err != nil {
				//TODO handle error
				log.Println("error closing elastic bulk request")
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
