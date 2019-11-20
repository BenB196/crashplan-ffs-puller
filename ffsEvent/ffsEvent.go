package ffsEvent

import (
	"bufio"
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
	"strconv"
	"strings"
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

	//Get initial authData
	authData, err := ffs.GetAuthData(configuration.AuthURI,query.Username,query.Password)

	if err != nil {
		log.Println("error getting auth data for ffs query: " + query.Name)
		panic(err)
	}

	//Make quit chan to close go routines
	quit := make(chan struct{})

	//Init goroutine for getting authData ever 55 minutes
	//Handle getting API AuthTokens every 55 minutes
	authTimeTicker := time.NewTicker(55 * time.Minute)
	go func() {
		for {
			select {
			case <- authTimeTicker.C:
				authData, err = ffs.GetAuthData(configuration.AuthURI,query.Username,query.Password)

				if err != nil {
					log.Println("error with getting authentication data for ffs query: " + query.Name)
					panic(err)
				}
			case <- quit:
				authTimeTicker.Stop()
				return
			}
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
				queryFetcher(query, &inProgressQueries, authData, configuration, &lastCompletedQuery, maxTime, true, elasticClient, ctx, nil, 0, false)
			}
		}()
	}

	//Handle setting the initial ON_OR_BEFORE and ON_OR_AFTER depending on the saved lastCompletedQuery
	if lastCompletedQuery != (eventOutput.InProgressQuery{}) {
		//TODO handle setting correct times
	}

	queryInterval, _ := time.ParseDuration(query.Interval)
	queryIntervalTimeTicker := time.NewTicker(queryInterval)
	wgQuery.Add(1)
	go func() {
		for {
			select {
			case <- queryIntervalTimeTicker.C:
				go queryFetcher(query, &inProgressQueries, authData, configuration, &lastCompletedQuery, maxTime,false, elasticClient, ctx, quit, 0, false)
			case <- quit:
				queryIntervalTimeTicker.Stop()
				wgQuery.Done()
				return
			}
		}
	}()
	wgQuery.Wait()
	return
}

func queryFetcher(query config.FFSQuery, inProgressQueries *[]eventOutput.InProgressQuery, authData ffs.AuthData, configuration config.Config, lastCompletedQuery *eventOutput.InProgressQuery, maxTime time.Time, cleanUpQuery bool, client *elastic.Client, ctx context.Context, quit chan<- struct{}, retryCount int, retryQuery bool) {
	var done bool
	var err error
	//Increment time
	//Only if it is not a catchup query (in progress queries when the app died)
	if !cleanUpQuery && !retryQuery {
		query, done, err = calculateTimeStamps(*inProgressQueries, *lastCompletedQuery, query, maxTime)

		if err != nil {
			panic(err)
		}

		//Stop the goroutine if the max time is past
		if done {
			if quit != nil {
				close(quit)
			}
			return
		}
	}

	//increase in progress queries
	if !retryQuery {
		promMetrics.IncreaseInProgressQueries()
	}

	//Add query interval to in progress query list
	inProgressQuery, err := getOnOrBeforeAndAfter(query)
	if err != nil {
		panic(err)
	}

	if !cleanUpQuery && !retryQuery {
		*inProgressQueries = append(*inProgressQueries,inProgressQuery)

		//Write in progress queries to file
		err := eventOutput.WriteInProgressQueries(query, inProgressQueries)

		if err != nil {
			panic(err)
		}
	}

	fileEvents, err := ffs.GetFileEvents(authData,configuration.FFSURI, query.Query)

	if err != nil {
		log.Println("error getting file events for ffs query: " + query.Name)
		//check if recoverable errors are thrown
		if strings.Contains(err.Error(),"Error with gathering file events POST: 500 Internal Server Error") || (strings.Contains(err.Error(),"stream error: stream ID") && (strings.Contains(err.Error(),"INTERNAL_ERROR") || strings.Contains(err.Error(),"PROTOCOL_ERROR"))) || strings.Contains(err.Error(),"read: connection reset by peer") || strings.Contains(err.Error(),"POST: 400 Bad Request") || strings.Contains(err.Error(),"unexpected EOF") || strings.Contains(err.Error(),"POST: 504 Gateway Timeout") || (strings.Contains(err.Error(),"record on line ") && strings.Contains(err.Error(),": wrong number of fields")) {
			//allow for 10 retries before killing to save resource overload.
			log.Println("Attempting to recover from error: " + err.Error() + ". Retry number: " + strconv.Itoa(retryCount))
			if retryCount <= 10 {
				queryInterval, _ := time.ParseDuration(query.Interval)
				//sleep before retry to reduce chance of hitting max queries per minute
				time.Sleep(queryInterval)
				retryCount = retryCount + 1
				queryFetcher(query, inProgressQueries, authData, configuration, lastCompletedQuery, maxTime, cleanUpQuery, client, ctx, quit, retryCount, true)
				return
			} else {
				//panic passed 10 retries
				panic("Failed on retry of query 10 times. Panicking to prevent unrecoverable resource utilization for ffs query: " + query.Name)
			}
		} else {
			//panic if unrecoverable/unknown error
			panic(err)
		}
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
						ffsEvents = append(ffsEvents,eventOutput.FFSEvent{FileEvent: event, Location: nil, GeoPoint: nil})
					} else if location, ok := locationMap[event.PublicIpAddress]; ok {
						//nil this as it is not needed, we already have event.publicIpAddress
						location.Query = ""
						geoPoint := eventOutput.GeoPoint{
							Lat: location.Lat,
							Lon: location.Lon,
						}
						ffsEvents = append(ffsEvents,eventOutput.FFSEvent{FileEvent: event, Location: &location, GeoPoint: &geoPoint})
					} else {
						b, _ := json.Marshal(event)
						log.Println("error getting location for fileEvent: " + string(b))
						panic("Unable to find location which should exist.")
					}
				} else {
					ffsEvents = append(ffsEvents,eventOutput.FFSEvent{FileEvent: event, Location: nil, GeoPoint: nil})
				}
				defer eventWg.Done()
			}
		}()

		//check if validIpAddressesOnly is true and if so convert all non-valid ip addresses to valid ones
		if query.ValidIpAddressesOnly {
			var validIpAddressesWg sync.WaitGroup
			validIpAddressesWg.Add(len(fileEvents))
			go func() {
				for i, fileEvent := range fileEvents {
					if len(fileEvent.PrivateIpAddresses) > 0 {
						var privateIpWg sync.WaitGroup
						privateIpWg.Add(len(fileEvent.PrivateIpAddresses))
						go func() {
							for x, privateIpAddress := range fileEvent.PrivateIpAddresses {
								fileEvents[i].PrivateIpAddresses[x] = strings.Split(privateIpAddress,"%")[0]
								privateIpWg.Done()
							}
						}()
						privateIpWg.Wait()
					}
					validIpAddressesWg.Done()
				}
			}()
			validIpAddressesWg.Wait()
		}

		eventWg.Wait()
		log.Println("Number of events for query: " + query.Name + " - " + strconv.Itoa(len(ffsEvents)))

		//remap ffsEvents to ElasticFFSEvent
		var elasticFFSEvents []eventOutput.ElasticFFSEvent
		var semiElasticFFSEvents []eventOutput.SemiElasticFFSEvent
		if query.EsStandardized != "" && strings.EqualFold(query.EsStandardized,"full") {
			var remapWg sync.WaitGroup
			remapWg.Add(len(ffsEvents))
			go func() {
				for _, ffsEvent := range ffsEvents {
					event := eventOutput.Event{
						EventId:        ffsEvent.EventId,
						EventType:      ffsEvent.EventType,
						EventTimestamp: ffsEvent.EventTimestamp,
					}

					insertion := eventOutput.Insertion{InsertionTimestamp:ffsEvent.InsertionTimestamp}

					file := eventOutput.File{
						FilePath:         ffsEvent.FilePath,
						FileName:         ffsEvent.FileName,
						FileType:         ffsEvent.FileType,
						FileCategory:     ffsEvent.FileCategory,
						FileSize:         ffsEvent.FileSize,
						FileOwner:        ffsEvent.FileOwner,
						Md5Checksum:      ffsEvent.Md5Checksum,
						Sha256Checksum:   ffsEvent.Sha256Checksum,
						CreatedTimestamp: ffsEvent.CreatedTimestamp,
						ModifyTimestamp:  ffsEvent.ModifyTimestamp,
					}

					device := eventOutput.Device{
						DeviceUsername:     ffsEvent.DeviceUsername,
						DeviceUid:          ffsEvent.DeviceUid,
						UserUid:            ffsEvent.UserUid,
						OsHostname:         ffsEvent.OsHostname,
						DomainName:         ffsEvent.DomainName,
						PublicIpAddress:    ffsEvent.PublicIpAddress,
						PrivateIpAddresses: ffsEvent.PrivateIpAddresses,
					}

					cloud := eventOutput.Cloud{
						Actor:                ffsEvent.Actor,
						DirectoryId:          ffsEvent.DirectoryId,
						Source:               ffsEvent.Source,
						Url:                  ffsEvent.Url,
						Shared:               ffsEvent.Shared,
						SharedWith:           ffsEvent.SharedWith,
						SharingTypeAdded:     ffsEvent.SharingTypeAdded,
						CloudDriveId:         ffsEvent.CloudDriveId,
						DetectionSourceAlias: ffsEvent.DetectionSourceAlias,
						FileId:               ffsEvent.FileId,
					}

					process := eventOutput.Process{
						ProcessOwner: ffsEvent.ProcessOwner,
						ProcessName:  ffsEvent.ProcessName,
						TabWindowTitle: ffsEvent.TabWindowTitle,
						TabUrl: ffsEvent.TabUrl,
					}

					removableMedia := eventOutput.RemovableMedia{
						RemovableMediaVendor:       ffsEvent.RemovableMediaVendor,
						RemovableMediaName:         ffsEvent.RemovableMediaName,
						RemovableMediaSerialNumber: ffsEvent.RemovableMediaSerialNumber,
						RemovableMediaCapacity:     ffsEvent.RemovableMediaCapacity,
						RemovableMediaBusType:      ffsEvent.RemovableMediaBusType,
						RemovableMediaMediaName:    ffsEvent.RemovableMediaMediaName,
						RemovableMediaVolumeName:   ffsEvent.RemovableMediaVolumeName,
						RemovableMediaPartitionId:  ffsEvent.RemovableMediaPartitionId,
					}

					email := eventOutput.Email{
						DLPPolicyName:			ffsEvent.EmailDLPPolicyName,
						DLPSubject:				ffsEvent.EmailDLPSubject,
						DLPSender:				ffsEvent.EmailDLPSender,
						DLPFrom:				ffsEvent.EmailDLPSender,
						DLPRecipients:			ffsEvent.EmailDLPRecipients,
					}

					elasticFileEvent := eventOutput.ElasticFileEvent{
						Event:						&event,
						Insertion:         			&insertion,
						File:						&file,
						Device:						&device,
						Cloud:						&cloud,
						Exposure:             		ffsEvent.Exposure,
						Process:					&process,
						RemovableMedia:				&removableMedia,
						SyncDestination:            ffsEvent.SyncDestination,
						Email:						&email,
					}

					var elasticFFSEvent eventOutput.ElasticFFSEvent
					var geoPoint eventOutput.GeoPoint
					var geoip eventOutput.Geoip
					if ffsEvent.Location != nil {
						if ffsEvent.Location.Lat != 0 && ffsEvent.Location.Lon != 0 {
							geoPoint = eventOutput.GeoPoint{
								Lat: ffsEvent.Location.Lat,
								Lon: ffsEvent.Location.Lon,
							}

							geoip = eventOutput.Geoip{
								Status:        ffsEvent.Location.Status,
								Message:       ffsEvent.Location.Message,
								Continent:     ffsEvent.Location.Continent,
								ContinentCode: ffsEvent.Location.ContinentCode,
								Country:       ffsEvent.Location.Country,
								CountryCode:   ffsEvent.Location.CountryCode,
								Region:        ffsEvent.Location.Region,
								RegionName:    ffsEvent.Location.RegionName,
								City:          ffsEvent.Location.City,
								District:      ffsEvent.Location.District,
								ZIP:           ffsEvent.Location.ZIP,
								Lat:           ffsEvent.Location.Lat,
								Lon:           ffsEvent.Location.Lon,
								Timezone:      ffsEvent.Location.Timezone,
								Currency:      ffsEvent.Location.Currency,
								ISP:           ffsEvent.Location.ISP,
								Org:           ffsEvent.Location.Org,
								AS:            ffsEvent.Location.AS,
								ASName:        ffsEvent.Location.ASName,
								Reverse:       ffsEvent.Location.Reverse,
								Mobile:        ffsEvent.Location.Mobile,
								Proxy:         ffsEvent.Location.Proxy,
								Query:         ffsEvent.Location.Query,
								GeoPoint:      &geoPoint,
							}
						} else {
							geoip = eventOutput.Geoip{
								Status:        ffsEvent.Location.Status,
								Message:       ffsEvent.Location.Message,
								Continent:     ffsEvent.Location.Continent,
								ContinentCode: ffsEvent.Location.ContinentCode,
								Country:       ffsEvent.Location.Country,
								CountryCode:   ffsEvent.Location.CountryCode,
								Region:        ffsEvent.Location.Region,
								RegionName:    ffsEvent.Location.RegionName,
								City:          ffsEvent.Location.City,
								District:      ffsEvent.Location.District,
								ZIP:           ffsEvent.Location.ZIP,
								Lat:           ffsEvent.Location.Lat,
								Lon:           ffsEvent.Location.Lon,
								Timezone:      ffsEvent.Location.Timezone,
								Currency:      ffsEvent.Location.Currency,
								ISP:           ffsEvent.Location.ISP,
								Org:           ffsEvent.Location.Org,
								AS:            ffsEvent.Location.AS,
								ASName:        ffsEvent.Location.ASName,
								Reverse:       ffsEvent.Location.Reverse,
								Mobile:        ffsEvent.Location.Mobile,
								Proxy:         ffsEvent.Location.Proxy,
								Query:         ffsEvent.Location.Query,
								GeoPoint:      nil,
							}
						}
					}

					if ffsEvent.Location != nil && ffsEvent.Location.Status == "" {
						elasticFFSEvent = eventOutput.ElasticFFSEvent{
							FileEvent: elasticFileEvent,
							Geoip:     nil,
						}
					} else {
						elasticFFSEvent = eventOutput.ElasticFFSEvent{
							FileEvent: elasticFileEvent,
							Geoip:     &geoip,
						}
					}

					elasticFFSEvents = append(elasticFFSEvents, elasticFFSEvent)
					remapWg.Done()
				}
			}()
			remapWg.Wait()
		} else if query.EsStandardized != "" && strings.EqualFold(query.EsStandardized,"half") {
			var remapWg sync.WaitGroup
			remapWg.Add(len(ffsEvents))
			go func() {
				for _, ffsEvent := range ffsEvents {
					semiElasticFileEvent := eventOutput.SemiElasticFileEvent{
						EventId:                    ffsEvent.EventId,
						EventType:                  ffsEvent.EventType,
						EventTimestamp:             ffsEvent.EventTimestamp,
						InsertionTimestamp:         ffsEvent.InsertionTimestamp,
						FilePath:                   ffsEvent.FilePath,
						FileName:                   ffsEvent.FileName,
						FileType:                   ffsEvent.FileType,
						FileCategory:               ffsEvent.FileCategory,
						FileSize:                   ffsEvent.FileSize,
						FileOwner:                  ffsEvent.FileOwner,
						Md5Checksum:                ffsEvent.Md5Checksum,
						Sha256Checksum:             ffsEvent.Sha256Checksum,
						CreatedTimestamp:           ffsEvent.CreatedTimestamp,
						ModifyTimestamp:            ffsEvent.ModifyTimestamp,
						DeviceUsername:             ffsEvent.DeviceUsername,
						DeviceUid:                  ffsEvent.DeviceUid,
						UserUid:                    ffsEvent.UserUid,
						OsHostname:                 ffsEvent.OsHostname,
						DomainName:                 ffsEvent.DomainName,
						PublicIpAddress:            ffsEvent.PublicIpAddress,
						PrivateIpAddresses:         ffsEvent.PrivateIpAddresses,
						Actor:                      ffsEvent.Actor,
						DirectoryId:                ffsEvent.DirectoryId,
						Source:                     ffsEvent.Source,
						Url:                        ffsEvent.Url,
						Shared:                     ffsEvent.Shared,
						SharedWith:                 ffsEvent.SharedWith,
						SharingTypeAdded:           ffsEvent.SharingTypeAdded,
						CloudDriveId:               ffsEvent.CloudDriveId,
						DetectionSourceAlias:       ffsEvent.DetectionSourceAlias,
						FileId:                     ffsEvent.FileId,
						Exposure:                   ffsEvent.Exposure,
						ProcessOwner:               ffsEvent.ProcessOwner,
						ProcessName:                ffsEvent.ProcessName,
						TabWindowTitle:				ffsEvent.TabWindowTitle,
						TabUrl:						ffsEvent.TabUrl,
						RemovableMediaVendor:       ffsEvent.RemovableMediaVendor,
						RemovableMediaName:         ffsEvent.RemovableMediaName,
						RemovableMediaSerialNumber: ffsEvent.RemovableMediaSerialNumber,
						RemovableMediaCapacity:     ffsEvent.RemovableMediaCapacity,
						RemovableMediaBusType:      ffsEvent.RemovableMediaBusType,
						RemovableMediaMediaName:    ffsEvent.RemovableMediaMediaName,
						RemovableMediaVolumeName:   ffsEvent.RemovableMediaVolumeName,
						RemovableMediaPartitionId:  ffsEvent.RemovableMediaPartitionId,
						SyncDestination:            ffsEvent.SyncDestination,
						EmailDLPPolicyName:			ffsEvent.EmailDLPPolicyName,
						EmailDLPSubject:			ffsEvent.EmailDLPSubject,
						EmailDLPSender:				ffsEvent.EmailDLPSender,
						EmailDLPFrom:				ffsEvent.EmailDLPSender,
						EmailDLPRecipients:			ffsEvent.EmailDLPRecipients,
					}

					var semiElasticFFSEvent eventOutput.SemiElasticFFSEvent
					var geoPoint eventOutput.GeoPoint
					var geoip eventOutput.Geoip
					if ffsEvent.Location != nil {
						if ffsEvent.Location.Lat != 0 && ffsEvent.Location.Lon != 0 {
							geoPoint = eventOutput.GeoPoint{
								Lat: ffsEvent.Location.Lat,
								Lon: ffsEvent.Location.Lon,
							}

							geoip = eventOutput.Geoip{
								Status:        ffsEvent.Location.Status,
								Message:       ffsEvent.Location.Message,
								Continent:     ffsEvent.Location.Continent,
								ContinentCode: ffsEvent.Location.ContinentCode,
								Country:       ffsEvent.Location.Country,
								CountryCode:   ffsEvent.Location.CountryCode,
								Region:        ffsEvent.Location.Region,
								RegionName:    ffsEvent.Location.RegionName,
								City:          ffsEvent.Location.City,
								District:      ffsEvent.Location.District,
								ZIP:           ffsEvent.Location.ZIP,
								Lat:           ffsEvent.Location.Lat,
								Lon:           ffsEvent.Location.Lon,
								Timezone:      ffsEvent.Location.Timezone,
								Currency:      ffsEvent.Location.Currency,
								ISP:           ffsEvent.Location.ISP,
								Org:           ffsEvent.Location.Org,
								AS:            ffsEvent.Location.AS,
								ASName:        ffsEvent.Location.ASName,
								Reverse:       ffsEvent.Location.Reverse,
								Mobile:        ffsEvent.Location.Mobile,
								Proxy:         ffsEvent.Location.Proxy,
								Query:         ffsEvent.Location.Query,
								GeoPoint:      &geoPoint,
							}
						} else {
							geoip = eventOutput.Geoip{
								Status:        ffsEvent.Location.Status,
								Message:       ffsEvent.Location.Message,
								Continent:     ffsEvent.Location.Continent,
								ContinentCode: ffsEvent.Location.ContinentCode,
								Country:       ffsEvent.Location.Country,
								CountryCode:   ffsEvent.Location.CountryCode,
								Region:        ffsEvent.Location.Region,
								RegionName:    ffsEvent.Location.RegionName,
								City:          ffsEvent.Location.City,
								District:      ffsEvent.Location.District,
								ZIP:           ffsEvent.Location.ZIP,
								Lat:           ffsEvent.Location.Lat,
								Lon:           ffsEvent.Location.Lon,
								Timezone:      ffsEvent.Location.Timezone,
								Currency:      ffsEvent.Location.Currency,
								ISP:           ffsEvent.Location.ISP,
								Org:           ffsEvent.Location.Org,
								AS:            ffsEvent.Location.AS,
								ASName:        ffsEvent.Location.ASName,
								Reverse:       ffsEvent.Location.Reverse,
								Mobile:        ffsEvent.Location.Mobile,
								Proxy:         ffsEvent.Location.Proxy,
								Query:         ffsEvent.Location.Query,
								GeoPoint:      nil,
							}
						}
					}

					if ffsEvent.Location != nil && ffsEvent.Location.Status == "" {
						semiElasticFFSEvent = eventOutput.SemiElasticFFSEvent{
							FileEvent: semiElasticFileEvent,
							Geoip:     nil,
						}
					} else {
						semiElasticFFSEvent = eventOutput.SemiElasticFFSEvent{
							FileEvent: semiElasticFileEvent,
							Geoip:     &geoip,
						}
					}

					semiElasticFFSEvents = append(semiElasticFFSEvents, semiElasticFFSEvent)
					remapWg.Done()
				}
			}()
			remapWg.Wait()
		}

		switch query.OutputType {
		case "file":
			if query.EsStandardized == "" {
				err = eventOutput.WriteEvents(ffsEvents, query)
			} else if query.EsStandardized == "full" {
				err = eventOutput.WriteEvents(elasticFFSEvents, query)
			} else if query.EsStandardized == "half" {
				err = eventOutput.WriteEvents(semiElasticFFSEvents, query)
			}

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
					var createIndex *elastic.IndicesCreateResult
					if !query.Elasticsearch.UseCustomIndexPattern {
						createIndex, err = client.CreateIndex(indexName).BodyString(elasticsearch.BuildIndexPattern(query.Elasticsearch)).Do(ctx)
					} else {
						createIndex, err = client.CreateIndex(indexName).Do(ctx)
					}

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
				if query.EsStandardized == "" {
					elasticWg.Add(len(ffsEvents))
					go func() {
						for _, ffsEvent := range ffsEvents {
							r := elastic.NewBulkIndexRequest().Index(indexName).Doc(ffsEvent)
							processor.Add(r)
							elasticWg.Done()
						}
					}()
				} else if query.EsStandardized == "full" {
					elasticWg.Add(len(elasticFFSEvents))
					go func() {
						for _, elasticFileEvent := range elasticFFSEvents {
							r := elastic.NewBulkIndexRequest().Index(indexName).Doc(elasticFileEvent)
							processor.Add(r)
							elasticWg.Done()
						}
					}()
				} else if query.EsStandardized == "half" {
					elasticWg.Add(len(semiElasticFFSEvents))
					go func() {
						for _, elasticFileEvent := range semiElasticFFSEvents {
							r := elastic.NewBulkIndexRequest().Index(indexName).Doc(elasticFileEvent)
							processor.Add(r)
							elasticWg.Done()
						}
					}()
				}

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
				if query.EsStandardized == "" {
					elasticWg.Add(len(ffsEvents))
					go func() {
						for _, ffsEvent := range ffsEvents {
							var indexTime time.Time
							if query.Elasticsearch.IndexTimeGen == "insertionTimestamp" {
								indexTime, _ = time.Parse(query.Elasticsearch.IndexTimeAppend,ffsEvent.EventTimestamp.Format(query.Elasticsearch.IndexTimeAppend))
							} else {
								indexTime, _ = time.Parse(query.Elasticsearch.IndexTimeAppend,ffsEvent.EventTimestamp.Format(query.Elasticsearch.IndexTimeAppend))
							}

							requiredIndexMutex.RLock()
							if _, found := requiredIndexTimestamps[indexTime]; !found {
								requiredIndexTimestamps[indexTime] = nil
							}
							requiredIndexMutex.RUnlock()
							elasticWg.Done()
						}
					}()
				} else if query.EsStandardized == "full" {
					elasticWg.Add(len(elasticFFSEvents))
					go func() {
						for _, elasticFileEvent := range elasticFFSEvents {
							var indexTime time.Time
							if query.Elasticsearch.IndexTimeGen == "insertionTimestamp" {
								indexTime, _ = time.Parse(query.Elasticsearch.IndexTimeAppend,elasticFileEvent.FileEvent.Event.EventTimestamp.Format(query.Elasticsearch.IndexTimeAppend))
							} else {
								indexTime, _ = time.Parse(query.Elasticsearch.IndexTimeAppend,elasticFileEvent.FileEvent.Event.EventTimestamp.Format(query.Elasticsearch.IndexTimeAppend))
							}

							requiredIndexMutex.RLock()
							if _, found := requiredIndexTimestamps[indexTime]; !found {
								requiredIndexTimestamps[indexTime] = nil
							}
							requiredIndexMutex.RUnlock()
							elasticWg.Done()
						}
					}()
				} else if query.EsStandardized == "half" {
					elasticWg.Add(len(semiElasticFFSEvents))
					go func() {
						for _, elasticFileEvent := range semiElasticFFSEvents {
							var indexTime time.Time
							if query.Elasticsearch.IndexTimeGen == "insertionTimestamp" {
								indexTime, _ = time.Parse(query.Elasticsearch.IndexTimeAppend,elasticFileEvent.FileEvent.EventTimestamp.Format(query.Elasticsearch.IndexTimeAppend))
							} else {
								indexTime, _ = time.Parse(query.Elasticsearch.IndexTimeAppend,elasticFileEvent.FileEvent.EventTimestamp.Format(query.Elasticsearch.IndexTimeAppend))
							}

							requiredIndexMutex.RLock()
							if _, found := requiredIndexTimestamps[indexTime]; !found {
								requiredIndexTimestamps[indexTime] = nil
							}
							requiredIndexMutex.RUnlock()
							elasticWg.Done()
						}
					}()
				}

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
							var createIndex *elastic.IndicesCreateResult
							if !query.Elasticsearch.UseCustomIndexPattern {
								createIndex, err = client.CreateIndex(indexName).BodyString(elasticsearch.BuildIndexPattern(query.Elasticsearch)).Do(ctx)
							} else {
								createIndex, err = client.CreateIndex(indexName).Do(ctx)
							}


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
				if query.EsStandardized == "" {
					elasticWg.Add(len(ffsEvents))
					go func() {
						for _, ffsEvent := range ffsEvents {
							var indexTime time.Time
							if query.Elasticsearch.IndexTimeGen == "insertionTimestamp" {
								indexTime, _ = time.Parse(query.Elasticsearch.IndexTimeAppend,ffsEvent.InsertionTimestamp.Format(query.Elasticsearch.IndexTimeAppend))
							} else {
								indexTime, _ = time.Parse(query.Elasticsearch.IndexTimeAppend,ffsEvent.EventTimestamp.Format(query.Elasticsearch.IndexTimeAppend))
							}
							indexName := elasticsearch.BuildIndexNameWithTime(query.Elasticsearch,indexTime)
							r := elastic.NewBulkIndexRequest().Index(indexName).Doc(ffsEvent)
							processor.Add(r)
							elasticWg.Done()
						}
					}()
				} else if query.EsStandardized == "full" {
					elasticWg.Add(len(elasticFFSEvents))
					go func() {
						for _, elasticFileEvent := range elasticFFSEvents {
							var indexTime time.Time
							if query.Elasticsearch.IndexTimeGen == "insertionTimestamp" {
								indexTime, _ = time.Parse(query.Elasticsearch.IndexTimeAppend,elasticFileEvent.FileEvent.Insertion.InsertionTimestamp.Format(query.Elasticsearch.IndexTimeAppend))
							} else {
								indexTime, _ = time.Parse(query.Elasticsearch.IndexTimeAppend,elasticFileEvent.FileEvent.Event.EventTimestamp.Format(query.Elasticsearch.IndexTimeAppend))
							}
							indexName := elasticsearch.BuildIndexNameWithTime(query.Elasticsearch,indexTime)
							r := elastic.NewBulkIndexRequest().Index(indexName).Doc(elasticFileEvent)
							processor.Add(r)
							elasticWg.Done()
						}
					}()
				} else if query.EsStandardized == "half" {
					elasticWg.Add(len(semiElasticFFSEvents))
					go func() {
						for _, elasticFileEvent := range semiElasticFFSEvents {
							var indexTime time.Time
							if query.Elasticsearch.IndexTimeGen == "insertionTimestamp" {
								indexTime, _ = time.Parse(query.Elasticsearch.IndexTimeAppend,elasticFileEvent.FileEvent.InsertionTimestamp.Format(query.Elasticsearch.IndexTimeAppend))
							} else {
								indexTime, _ = time.Parse(query.Elasticsearch.IndexTimeAppend,elasticFileEvent.FileEvent.EventTimestamp.Format(query.Elasticsearch.IndexTimeAppend))
							}
							indexName := elasticsearch.BuildIndexNameWithTime(query.Elasticsearch,indexTime)
							r := elastic.NewBulkIndexRequest().Index(indexName).Doc(elasticFileEvent)
							processor.Add(r)
							elasticWg.Done()
						}
					}()
				}
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
		case "logstash":
			var logstashWg sync.WaitGroup

			conn, err := elasticsearch.CreateLogstashClient(query.Logstash.LogstashURL)

			if err != nil {
				//TODO handle error
				log.Println("error creating logstash connection")
				panic(err)
			}

			writer := bufio.NewWriter(conn)

			if query.EsStandardized == "" {
				logstashWg.Add(len(ffsEvents))
				go func() {
					for _, ffsEvent := range ffsEvents {
						event, err := json.Marshal(ffsEvent)

						if err != nil {
							//TODO handle error
							log.Println("error marshaling ffs event")
							log.Println(ffsEvent)
							panic(err)
						}

						_, err = writer.Write(event)

						if err != nil {
							//TODO handle error
							log.Println("error writing ffs event")
							log.Println(string(event))
							panic(err)
						}
						_, err = writer.Write([]byte("\n"))
						if err != nil {
							//TODO handle error
							log.Println("error writing ffs event")
							log.Println(string(event))
							panic(err)
						}
						logstashWg.Done()
					}
				}()
			} else if query.EsStandardized == "full" {
				logstashWg.Add(len(elasticFFSEvents))
				go func() {
					for _, elasticFileEvent := range elasticFFSEvents {
						event, err := json.Marshal(elasticFileEvent)

						if err != nil {
							//TODO handle error
							log.Println("error marshaling ffs event")
							log.Println(elasticFileEvent)
							panic(err)
						}

						_, err = writer.Write(event)

						if err != nil {
							//TODO handle error
							log.Println("error writing ffs event")
							log.Println(string(event))
							panic(err)
						}
						_, err = writer.Write([]byte("\n"))
						if err != nil {
							//TODO handle error
							log.Println("error writing ffs event")
							log.Println(string(event))
							panic(err)
						}
						logstashWg.Done()
					}
				}()
			} else if query.EsStandardized == "half" {
				logstashWg.Add(len(semiElasticFFSEvents))
				go func() {
					for _, elasticFileEvent := range semiElasticFFSEvents {
						event, err := json.Marshal(elasticFileEvent)

						if err != nil {
							//TODO handle error
							log.Println("error marshaling ffs event")
							log.Println(elasticFileEvent)
							panic(err)
						}

						_, err = writer.Write(event)

						if err != nil {
							//TODO handle error
							log.Println("error writing ffs event")
							log.Println(string(event))
							panic(err)
						}
						_, err = writer.Write([]byte("\n"))
						if err != nil {
							//TODO handle error
							log.Println("error writing ffs event")
							log.Println(string(event))
							panic(err)
						}
						logstashWg.Done()
					}
				}()
			}


			logstashWg.Wait()

			err = writer.Flush()

			if err != nil {
				log.Println("error flushing logstash buffer")
				panic(err)
			}

			err = conn.Close()

			if err != nil {
				log.Println("error closing logstash connection")
				panic(err)
			}
		}
	}

	//Check if this query is the newest completed query, if it is, set last completed query to query times
	if lastCompletedQuery.OnOrBefore.Sub(inProgressQuery.OnOrAfter) <= 0 {
		*lastCompletedQuery = inProgressQuery

		err := eventOutput.WriteLastCompletedQuery(query, inProgressQuery)
		if err != nil {
			panic(err)
		}
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

	//Write in progress queries to file
	err = eventOutput.WriteInProgressQueries(query, inProgressQueries)

	if err != nil {
		panic(err)
	}

	promMetrics.IncrementEventsProcessed(len(ffsEvents))
	promMetrics.DecreaseInProgressQueries()
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
