package ffsEvent

import (
	"bufio"
	"context"
	"encoding/json"
	"github.com/BenB196/crashplan-ffs-go-pkg"
	"github.com/BenB196/crashplan-ffs-puller/config"
	"github.com/BenB196/crashplan-ffs-puller/elasticsearch"
	"github.com/BenB196/crashplan-ffs-puller/eventOutput"
	"github.com/BenB196/crashplan-ffs-puller/promMetrics"
	ip_api "github.com/BenB196/ip-api-go-pkg"
	"github.com/olivere/elastic/v7"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

func queryFetcher(query config.FFSQuery, inProgressQueries *[]eventOutput.InProgressQuery, authData ffs.AuthData, configuration config.Config, lastCompletedQuery *eventOutput.InProgressQuery, maxTime time.Time, cleanUpQuery bool, client *elastic.Client, ctx context.Context, quit chan<- struct{}, retryCount int, retryQuery bool) {
	startTime := time.Now()
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
	cleanUpQueryTime := time.Now()

	//increase in progress queries
	if !retryQuery {
		promMetrics.IncreaseInProgressQueries()
	}

	//Add query interval to in progress query list
	inProgressQuery, err := getOnOrBeforeAndAfter(query)
	if err != nil {
		panic(err)
	}

	onOrBeforeAndAfterTime := time.Now()

	if !cleanUpQuery && !retryQuery {
		*inProgressQueries = append(*inProgressQueries, *inProgressQuery)

		//Write in progress queries to file
		err := eventOutput.WriteInProgressQueries(query, *inProgressQueries)

		if err != nil {
			panic(err)
		}
	}

	notInProgressTime := time.Now()

	fileEvents, err := ffs.GetFileEvents(authData, configuration.FFSURI, query.Query)

	getFileEventsTime := time.Now()

	if err != nil {
		log.Println("error getting file events for ffs query: " + query.Name)
		//check if recoverable errors are thrown
		if strings.Contains(err.Error(), "Error with gathering file events POST: 500 Internal Server Error") || (strings.Contains(err.Error(), "stream error: stream ID") && (strings.Contains(err.Error(), "INTERNAL_ERROR") || strings.Contains(err.Error(), "PROTOCOL_ERROR"))) || strings.Contains(err.Error(), "read: connection reset by peer") || strings.Contains(err.Error(), "POST: 400 Bad Request") || strings.Contains(err.Error(), "unexpected EOF") || strings.Contains(err.Error(), "POST: 504 Gateway Timeout") || (strings.Contains(err.Error(), "record on line ") && strings.Contains(err.Error(), ": wrong number of fields") || (strings.Contains(err.Error(), "record on line") && strings.Contains(err.Error(), "; parse error on line") && strings.Contains(err.Error(), ", column") && strings.Contains(err.Error(), ": extraneous or missing \" in quoted-field"))) {
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

	retryGetFileEventsTime := time.Now()

	//Write events
	var ffsEvents []eventOutput.FFSEvent

	var enrichmentTime time.Time

	if len(*fileEvents) > 0 {
		//remap ffsEvents to ElasticFFSEvent
		var elasticFFSEvents []eventOutput.ElasticFileEvent
		var semiElasticFFSEvents []eventOutput.SemiElasticFFSEvent
		var remapWg sync.WaitGroup
		remapWg.Add(len(*fileEvents))
		go func() {
			for _, ffsEvent := range *fileEvents {
				//IP API Lookup
				var location *ip_api.Location
				if configuration.IPAPI.Enabled {
					location = getIpApiLocation(configuration, ffsEvent.PublicIpAddress)
				}

				//convert to valid IP addresses if enabled
				if query.ValidIpAddressesOnly && len(ffsEvent.PrivateIpAddresses) > 0 {
					for x, privateIpAddress := range ffsEvent.PrivateIpAddresses {
						ffsEvent.PrivateIpAddresses[x] = strings.Split(privateIpAddress, "%")[0]
					}
				}

				if query.EsStandardized != "" && strings.EqualFold(query.EsStandardized, "full") {
					event := &eventOutput.Event{
						Id:                 ffsEvent.EventId,
						Type:               ffsEvent.EventType,
						Ingested:           ffsEvent.InsertionTimestamp,
						Created:            ffsEvent.EventTimestamp,
						Module:             ffsEvent.Source,
						Dataset:            ffsEvent.Exposure,
						OutsideActiveHours: ffsEvent.OutsideActiveHours,
					}

					timestamp := ffsEvent.EventTimestamp

					var extensions []string

					if ffsEvent.IdentifiedExtensionCategory != "" {
						extensions = append(extensions, ffsEvent.IdentifiedExtensionCategory)
					}

					if ffsEvent.CurrentExtensionCategory != "" {
						extensions = append(extensions, ffsEvent.CurrentExtensionCategory)
					}

					hash := &eventOutput.Hash{
						Md5:    ffsEvent.Md5Checksum,
						Sha256: ffsEvent.Sha256Checksum,
					}

					if *hash == (eventOutput.Hash{}) {
						hash = nil
					}

					url := getUrlInfo(ffsEvent.Url)

					file := &eventOutput.File{
						Path:                        ffsEvent.FilePath,
						Name:                        ffsEvent.FileName,
						Type:                        ffsEvent.FileType,
						Category:                    ffsEvent.FileCategory,
						IdentifiedExtensionCategory: ffsEvent.IdentifiedExtensionCategory,
						CurrentExtensionCategory:    ffsEvent.CurrentExtensionCategory,
						Extension:                   extensions,
						Size:                        ffsEvent.FileSize,
						Owner:                       ffsEvent.FileOwner,
						Hash:                        hash,
						Created:                     ffsEvent.CreatedTimestamp,
						Mtime:                       ffsEvent.ModifyTimestamp,
						Directory:                   ffsEvent.DirectoryId,
						URL:                         url,
						Shared:                      ffsEvent.Shared,
						SharedWith:                  ffsEvent.SharedWith,
						SharingTypeAdded:            ffsEvent.SharingTypeAdded,
						CloudDriveId:                ffsEvent.CloudDriveId,
						DetectionSourceAlias:        ffsEvent.DetectionSourceAlias,
						SyncDestination:             ffsEvent.SyncDestination,
						Id:                          ffsEvent.FileId,
						IdentifiedExtensionMIMEType: ffsEvent.IdentifiedExtensionMIMEType,
						CurrentExtensionMIMEType:    ffsEvent.CurrentExtensionMIMEType,
						SuspiciousFileTypeMismatch:  ffsEvent.SuspiciousFileTypeMismatch,
						RemoteActivity: ffsEvent.RemoteActivity,
						Trusted: ffsEvent.Trusted,
					}

					var user *eventOutput.User

					if ffsEvent.DeviceUsername == "NAME_NOT_AVAILABLE" {
						user = &eventOutput.User{
							Email: ffsEvent.Actor,
							Id:    ffsEvent.UserUid,
							Actor: ffsEvent.Actor,
						}
					} else {
						user = &eventOutput.User{
							Email: ffsEvent.DeviceUsername,
							Id:    ffsEvent.UserUid,
							Actor: ffsEvent.Actor,
						}
					}

					if *user == (eventOutput.User{}) {
						user = nil
					}

					host := &eventOutput.Host{
						Id:       ffsEvent.DeviceUid,
						Name:     ffsEvent.OsHostname,
						Hostname: ffsEvent.DomainName,
					}

					if *host == (eventOutput.Host{}) {
						host = nil
					}

					var nat *eventOutput.Nat

					if ffsEvent.PrivateIpAddresses != nil {
						nat = &eventOutput.Nat{Ip: ffsEvent.PrivateIpAddresses}
					} else {
						nat = nil
					}

					var geo *eventOutput.Geo
					var as *eventOutput.AS
					if location != nil {
						geo = &eventOutput.Geo{
							Status:        location.Status,
							Message:       location.Message,
							Continent:     location.Continent,
							ContinentCode: location.ContinentCode,
							Country:       location.Country,
							CountryCode:   location.CountryCode,
							Region:        location.Region,
							RegionName:    location.RegionName,
							City:          location.City,
							District:      location.District,
							ZIP:           location.ZIP,
							Lat:           location.Lat,
							Lon:           location.Lon,
							Timezone:      location.Timezone,
							Currency:      location.Currency,
							ISP:           location.ISP,
							Org:           location.Org,
							AS:            location.AS,
							ASName:        location.ASName,
							Reverse:       location.Reverse,
							Mobile:        location.Mobile,
							Proxy:         location.Proxy,
							Hosting:       location.Hosting,
							Query:         location.Query,
						}

						if (location.Lat != nil && *location.Lat != 0) && (location.Lon != nil && *location.Lon != 0) {
							geo.Location = &eventOutput.Location{
								Lat: location.Lat,
								Lon: location.Lon,
							}
						} else {
							geo.Location = nil
						}

						if geo.ISP != "" {
							as = &eventOutput.AS{Organization: &eventOutput.Organization{Name: geo.ISP}}
						} else {
							as = nil
						}
					} else {
						geo = nil
					}

					client := &eventOutput.Client{
						Ip:  ffsEvent.PublicIpAddress,
						Nat: nat,
						Geo: geo,
						AS:  as,
					}

					if *client == (eventOutput.Client{}) {
						client = nil
					}

					process := &eventOutput.Process{
						ProcessOwner: ffsEvent.ProcessOwner,
						ProcessName:  ffsEvent.ProcessName,
					}

					if *process == (eventOutput.Process{}) {
						process = nil
					}

					tabUrl := getUrlInfo(ffsEvent.TabUrl)

					tab := &eventOutput.Tab{
						WindowTitle: ffsEvent.TabWindowTitle,
						URL:         tabUrl,
					}

					if *tab == (eventOutput.Tab{}) {
						tab = nil
					}

					removableMedia := &eventOutput.RemovableMedia{
						Vendor:       ffsEvent.RemovableMediaVendor,
						Name:         ffsEvent.RemovableMediaName,
						SerialNumber: ffsEvent.RemovableMediaSerialNumber,
						Capacity:     ffsEvent.RemovableMediaCapacity,
						BusType:      ffsEvent.RemovableMediaBusType,
						MediaName:    ffsEvent.RemovableMediaMediaName,
						VolumeName:   ffsEvent.RemovableMediaVolumeName,
						PartitionId:  ffsEvent.RemovableMediaPartitionId,
					}

					if *removableMedia == (eventOutput.RemovableMedia{}) {
						removableMedia = nil
					}

					emailDlp := &eventOutput.EmailDlp{
						PolicyNames: ffsEvent.EmailDLPPolicyNames,
						Subject:     ffsEvent.EmailDLPSubject,
						Sender:      ffsEvent.EmailDLPSender,
						From:        ffsEvent.EmailDLPFrom,
						Recipients:  ffsEvent.EmailDLPRecipients,
					}

					if ffsEvent.EmailDLPPolicyNames == nil && ffsEvent.EmailDLPSubject == "" && ffsEvent.EmailDLPSender == "" && ffsEvent.EmailDLPFrom == "" && ffsEvent.EmailDLPRecipients == nil {
						emailDlp = nil
					}

					printer := &eventOutput.Printer{
						Name: ffsEvent.PrinterName,
					}

					if ffsEvent.PrinterName == "" {
						printer = nil
					}

					printing := &eventOutput.Printing{
						JobName:                ffsEvent.PrintJobName,
						Printer:                printer,
						PrintedFilesBackupPath: ffsEvent.PrintedFilesBackupPath,
					}

					if *printing == (eventOutput.Printing{}) {
						printing = nil
					}

					elasticFileEvent := &eventOutput.ElasticFileEvent{
						Event:          event,
						Timestamp:      timestamp,
						File:           file,
						User:           user,
						Host:           host,
						Client:         client,
						Process:        process,
						Tab:            tab,
						RemovableMedia: removableMedia,
						EmailDlp:       emailDlp,
						Printing:       printing,
					}

					elasticFFSEvents = append(elasticFFSEvents, *elasticFileEvent)
					remapWg.Done()
				} else if query.EsStandardized != "" && strings.EqualFold(query.EsStandardized, "half") {
					semiElasticFileEvent := eventOutput.SemiElasticFileEvent{
						EventId:                     ffsEvent.EventId,
						EventType:                   ffsEvent.EventType,
						EventTimestamp:              ffsEvent.EventTimestamp,
						InsertionTimestamp:          ffsEvent.InsertionTimestamp,
						FilePath:                    ffsEvent.FilePath,
						FileName:                    ffsEvent.FileName,
						FileType:                    ffsEvent.FileType,
						FileCategory:                ffsEvent.FileCategory,
						IdentifiedExtensionCategory: ffsEvent.IdentifiedExtensionCategory,
						CurrentExtensionCategory:    ffsEvent.CurrentExtensionCategory,
						FileSize:                    ffsEvent.FileSize,
						FileOwner:                   ffsEvent.FileOwner,
						Md5Checksum:                 ffsEvent.Md5Checksum,
						Sha256Checksum:              ffsEvent.Sha256Checksum,
						CreatedTimestamp:            ffsEvent.CreatedTimestamp,
						ModifyTimestamp:             ffsEvent.ModifyTimestamp,
						DeviceUsername:              ffsEvent.DeviceUsername,
						DeviceUid:                   ffsEvent.DeviceUid,
						UserUid:                     ffsEvent.UserUid,
						OsHostname:                  ffsEvent.OsHostname,
						DomainName:                  ffsEvent.DomainName,
						PublicIpAddress:             ffsEvent.PublicIpAddress,
						PrivateIpAddresses:          ffsEvent.PrivateIpAddresses,
						Actor:                       ffsEvent.Actor,
						DirectoryId:                 ffsEvent.DirectoryId,
						Source:                      ffsEvent.Source,
						Url:                         ffsEvent.Url,
						Shared:                      ffsEvent.Shared,
						SharedWith:                  ffsEvent.SharedWith,
						SharingTypeAdded:            ffsEvent.SharingTypeAdded,
						CloudDriveId:                ffsEvent.CloudDriveId,
						DetectionSourceAlias:        ffsEvent.DetectionSourceAlias,
						FileId:                      ffsEvent.FileId,
						Exposure:                    ffsEvent.Exposure,
						ProcessOwner:                ffsEvent.ProcessOwner,
						ProcessName:                 ffsEvent.ProcessName,
						TabWindowTitle:              ffsEvent.TabWindowTitle,
						TabUrl:                      ffsEvent.TabUrl,
						RemovableMediaVendor:        ffsEvent.RemovableMediaVendor,
						RemovableMediaName:          ffsEvent.RemovableMediaName,
						RemovableMediaSerialNumber:  ffsEvent.RemovableMediaSerialNumber,
						RemovableMediaCapacity:      ffsEvent.RemovableMediaCapacity,
						RemovableMediaBusType:       ffsEvent.RemovableMediaBusType,
						RemovableMediaMediaName:     ffsEvent.RemovableMediaMediaName,
						RemovableMediaVolumeName:    ffsEvent.RemovableMediaVolumeName,
						RemovableMediaPartitionId:   ffsEvent.RemovableMediaPartitionId,
						SyncDestination:             ffsEvent.SyncDestination,
						EmailDLPPolicyNames:         ffsEvent.EmailDLPPolicyNames,
						EmailDLPSubject:             ffsEvent.EmailDLPSubject,
						EmailDLPSender:              ffsEvent.EmailDLPSender,
						EmailDLPFrom:                ffsEvent.EmailDLPSender,
						EmailDLPRecipients:          ffsEvent.EmailDLPRecipients,
						OutsideActiveHours:          ffsEvent.OutsideActiveHours,
						IdentifiedExtensionMIMEType: ffsEvent.IdentifiedExtensionMIMEType,
						CurrentExtensionMIMEType:    ffsEvent.CurrentExtensionMIMEType,
						SuspiciousFileTypeMismatch:  ffsEvent.SuspiciousFileTypeMismatch,
						PrintJobName:                ffsEvent.PrintJobName,
						PrinterName:                 ffsEvent.PrinterName,
						PrintedFilesBackupPath:      ffsEvent.PrintedFilesBackupPath,
						RemoteActivity: ffsEvent.RemoteActivity,
						Trusted: ffsEvent.Trusted,
					}

					var semiElasticFFSEvent eventOutput.SemiElasticFFSEvent
					var geo *eventOutput.Geo
					if location != nil {
						geo = &eventOutput.Geo{
							Status:        location.Status,
							Message:       location.Message,
							Continent:     location.Continent,
							ContinentCode: location.ContinentCode,
							Country:       location.Country,
							CountryCode:   location.CountryCode,
							Region:        location.Region,
							RegionName:    location.RegionName,
							City:          location.City,
							District:      location.District,
							ZIP:           location.ZIP,
							Lat:           location.Lat,
							Lon:           location.Lon,
							Timezone:      location.Timezone,
							Currency:      location.Currency,
							ISP:           location.ISP,
							Org:           location.Org,
							AS:            location.AS,
							ASName:        location.ASName,
							Reverse:       location.Reverse,
							Mobile:        location.Mobile,
							Proxy:         location.Proxy,
							Hosting:       location.Hosting,
							Query:         location.Query,
						}

						if (location.Lat != nil && *location.Lat != 0) && (location.Lon != nil && *location.Lon != 0) {
							geo.Location = &eventOutput.Location{
								Lat: location.Lat,
								Lon: location.Lon,
							}
						} else {
							geo.Location = nil
						}
					}

					semiElasticFFSEvent = eventOutput.SemiElasticFFSEvent{
						FileEvent: semiElasticFileEvent,
					}

					if location != nil && location.Status == "" {
						semiElasticFFSEvent.Geo = nil
					} else {
						semiElasticFFSEvent.Geo = geo
					}

					semiElasticFFSEvents = append(semiElasticFFSEvents, semiElasticFFSEvent)
					remapWg.Done()
				}
			}
		}()
		remapWg.Wait()

		enrichmentTime = time.Now()

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
					indexName = elasticsearch.BuildIndexNameWithTime(query.Elasticsearch, inProgressQuery.OnOrBefore)
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
								indexTime, _ = time.Parse(query.Elasticsearch.IndexTimeAppend, ffsEvent.EventTimestamp.Format(query.Elasticsearch.IndexTimeAppend))
							} else {
								indexTime, _ = time.Parse(query.Elasticsearch.IndexTimeAppend, ffsEvent.EventTimestamp.Format(query.Elasticsearch.IndexTimeAppend))
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
								indexTime, _ = time.Parse(query.Elasticsearch.IndexTimeAppend, elasticFileEvent.Event.Created.Format(query.Elasticsearch.IndexTimeAppend))
							} else {
								indexTime, _ = time.Parse(query.Elasticsearch.IndexTimeAppend, elasticFileEvent.Event.Created.Format(query.Elasticsearch.IndexTimeAppend))
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
								indexTime, _ = time.Parse(query.Elasticsearch.IndexTimeAppend, elasticFileEvent.FileEvent.EventTimestamp.Format(query.Elasticsearch.IndexTimeAppend))
							} else {
								indexTime, _ = time.Parse(query.Elasticsearch.IndexTimeAppend, elasticFileEvent.FileEvent.EventTimestamp.Format(query.Elasticsearch.IndexTimeAppend))
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
						indexName := elasticsearch.BuildIndexNameWithTime(query.Elasticsearch, timestamp)
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
								indexTime, _ = time.Parse(query.Elasticsearch.IndexTimeAppend, ffsEvent.InsertionTimestamp.Format(query.Elasticsearch.IndexTimeAppend))
							} else {
								indexTime, _ = time.Parse(query.Elasticsearch.IndexTimeAppend, ffsEvent.EventTimestamp.Format(query.Elasticsearch.IndexTimeAppend))
							}
							indexName := elasticsearch.BuildIndexNameWithTime(query.Elasticsearch, indexTime)
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
								indexTime, _ = time.Parse(query.Elasticsearch.IndexTimeAppend, elasticFileEvent.Event.Ingested.Format(query.Elasticsearch.IndexTimeAppend))
							} else {
								indexTime, _ = time.Parse(query.Elasticsearch.IndexTimeAppend, elasticFileEvent.Event.Created.Format(query.Elasticsearch.IndexTimeAppend))
							}
							indexName := elasticsearch.BuildIndexNameWithTime(query.Elasticsearch, indexTime)
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
								indexTime, _ = time.Parse(query.Elasticsearch.IndexTimeAppend, elasticFileEvent.FileEvent.InsertionTimestamp.Format(query.Elasticsearch.IndexTimeAppend))
							} else {
								indexTime, _ = time.Parse(query.Elasticsearch.IndexTimeAppend, elasticFileEvent.FileEvent.EventTimestamp.Format(query.Elasticsearch.IndexTimeAppend))
							}
							indexName := elasticsearch.BuildIndexNameWithTime(query.Elasticsearch, indexTime)
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
	outputTime := time.Now()

	//Check if this query is the newest completed query, if it is, set last completed query to query times
	if lastCompletedQuery.OnOrBefore.Sub(inProgressQuery.OnOrAfter) <= 0 {
		*lastCompletedQuery = *inProgressQuery

		err := eventOutput.WriteLastCompletedQuery(query, *inProgressQuery)
		if err != nil {
			panic(err)
		}
	}

	writeLastCompletedQueryTime := time.Now()

	//Remove from in progress query slice
	temp := *inProgressQueries
	tempInProgress := temp[:0]
	for _, query := range temp {
		if query.OnOrAfter != inProgressQuery.OnOrAfter && query.OnOrBefore != inProgressQuery.OnOrBefore {
			tempInProgress = append(tempInProgress, query)
		}
	}
	*inProgressQueries = tempInProgress

	//Write in progress queries to file
	err = eventOutput.WriteInProgressQueries(query, *inProgressQueries)

	removeInProgressQueryTime := time.Now()

	if err != nil {
		panic(err)
	}

	promMetrics.IncrementEventsProcessed(len(*fileEvents))
	promMetrics.DecreaseInProgressQueries()
	endTime := time.Now()
	duration := endTime.Sub(startTime)
	cleanupDuration := cleanUpQueryTime.Sub(startTime)
	onOrBeforeAndAfterDuration := onOrBeforeAndAfterTime.Sub(cleanUpQueryTime)
	notInProgressDuration := notInProgressTime.Sub(onOrBeforeAndAfterTime)
	getFileEventsDuration := getFileEventsTime.Sub(notInProgressTime)
	retryGetFileEventsDuration := retryGetFileEventsTime.Sub(getFileEventsTime)
	enrichmentDuration := enrichmentTime.Sub(retryGetFileEventsTime)
	outputDuration := outputTime.Sub(enrichmentTime)
	writeLastCompletedQueryDuration := writeLastCompletedQueryTime.Sub(outputTime)
	removeInProgressQueryDuration := removeInProgressQueryTime.Sub(writeLastCompletedQueryTime)
	log.Println("Number of events for query: " + query.Name + " - " + strconv.Itoa(len(*fileEvents)) +
		" - Clean Up Duration: " + cleanupDuration.String() + " - " +
		"On Or Before And After Duration: " + onOrBeforeAndAfterDuration.String() + " - Not In-progress Duration: " +
		notInProgressDuration.String() + " - Get File Events Duration: " + getFileEventsDuration.String() + " - Retry Get File Events Duration: " +
		retryGetFileEventsDuration.String() + " - Enrichment Duration: " + enrichmentDuration.String() + " - Output Duration: " + outputDuration.String() +
		" - Write Last Completed Query Duration: " + writeLastCompletedQueryDuration.String() +
		" - Remove In Progress Query Duration: " + removeInProgressQueryDuration.String() + " - Duration: " + duration.String())
}
