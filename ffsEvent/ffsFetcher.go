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
	"github.com/BenB196/ip-api-go-pkg"
	"github.com/olivere/elastic/v7"
	"log"
	"path/filepath"
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

	fileEvents, _, err := ffs.GetJsonFileEvents(authData, configuration.FFSURI, query.Query, "", configuration.Debugging)

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

				if query.EsStandardized != "" && strings.EqualFold(query.EsStandardized, "ecs") {

					//Event processing
					eventType := "info"
					if strings.EqualFold(ffsEvent.EventType, "created") {
						eventType = "creation"
					} else if strings.EqualFold(ffsEvent.EventType, "modified") {
						eventType = "change"
					} else if strings.EqualFold(ffsEvent.EventType, "deleted") {
						eventType = "deletion"
					}

					//eventTimestamp
					if len(strings.Split(ffsEvent.EventTimestamp, ".")) != 2 {
						ffsEvent.EventTimestamp = ffsEvent.EventTimestamp + ".000"
					}
					eventTimestamp, err := time.Parse("2006-01-02 15:04:05.000", strings.Replace(strings.Replace(ffsEvent.EventTimestamp, "T", " ", -1), "Z", "", -1))

					if err != nil {
						panic(err)
					}

					//insertionTimestamp

					if len(strings.Split(ffsEvent.InsertionTimestamp, ".")) != 2 {
						ffsEvent.InsertionTimestamp = ffsEvent.InsertionTimestamp + ".000"
					}

					insertionTimeSplit := strings.Split(ffsEvent.InsertionTimestamp, ".")
					insertionTimestampLength := len(insertionTimeSplit[1])
					if insertionTimestampLength > 4 {
						ffsEvent.InsertionTimestamp = insertionTimeSplit[0] + "." + insertionTimeSplit[1][0:3]
					}

					insertionTimestamp, err := time.Parse("2006-01-02 15:04:05.000", strings.Replace(strings.Replace(ffsEvent.InsertionTimestamp, "T", " ", -1), "Z", "", -1))
					if err != nil {
						panic(err)
					}

					//creationTimestamp
					var createTimestamp time.Time
					if ffsEvent.CreateTimestamp != "" {
						if len(strings.Split(ffsEvent.CreateTimestamp, ".")) != 2 {
							ffsEvent.CreateTimestamp = ffsEvent.CreateTimestamp + ".000"
						}

						createTimestamp, err = time.Parse("2006-01-02 15:04:05.000", strings.Replace(strings.Replace(ffsEvent.CreateTimestamp, "T", " ", -1), "Z", "", -1))

						if err != nil {
							panic(err)
						}
					}

					//modifyTimestamp
					var modifyTimestamp time.Time
					if ffsEvent.ModifyTimestamp != "" {
						if len(strings.Split(ffsEvent.ModifyTimestamp, ".")) != 2 {
							ffsEvent.ModifyTimestamp = ffsEvent.ModifyTimestamp + ".000"
						}

						modifyTimestamp, err = time.Parse("2006-01-02 15:04:05.000", strings.Replace(strings.Replace(ffsEvent.ModifyTimestamp, "T", " ", -1), "Z", "", -1))

						if err != nil {
							panic(err)
						}
					}


					event := &eventOutput.Event{
						Action:   ffsEvent.EventType,
						Category: "file",
						Created:  &eventTimestamp,
						Dataset:  "code42.ffs",
						Id:       ffsEvent.EventId,
						Ingested: &insertionTimestamp,
						Kind:     "event",
						Module:   "code42",
						Type:     eventType,
					}

					//@timestamp
					timestamp := eventTimestamp

					//file fields
					hash := &eventOutput.Hash{
						Md5:    ffsEvent.Md5Checksum,
						Sha256: ffsEvent.Sha256Checksum,
					}

					if *hash == (eventOutput.Hash{}) {
						hash = nil
					}

					fileType := "unknown"
					if strings.EqualFold(ffsEvent.FileType, "file") || strings.EqualFold(ffsEvent.FileType, "win_nds") || strings.EqualFold(ffsEvent.FileType, "mac_rsrc") || strings.EqualFold(ffsEvent.FileType, "fifo") || strings.EqualFold(ffsEvent.FileType, "bundle") {
						fileType = "file"
					} else if strings.EqualFold(ffsEvent.FileType, "dir") || strings.EqualFold(ffsEvent.FileType, "block_device") || strings.EqualFold(ffsEvent.FileType, "char_device") {
						fileType = "dir"
					} else if strings.EqualFold(ffsEvent.FileType, "symlink") {
						fileType = "symlink"
					}

					file := &eventOutput.File{
						Path:      ffsEvent.FilePath,
						Name:      ffsEvent.FileName,
						Type:      fileType,
						Extension: strings.Replace(filepath.Ext(ffsEvent.FileName),".","",-1),
						Size:      ffsEvent.FileSize,
						Owner:     ffsEvent.FileOwner,
						Hash:      hash,
						Created:   &createTimestamp,
						Mtime:     &modifyTimestamp,
						Directory: ffsEvent.DirectoryId,
						MimeType:  []string{ffsEvent.MimeTypeByBytes, ffsEvent.MimeTypeByExtension},
					}

					//user fields
					var user *eventOutput.User

					if ffsEvent.DeviceUserName == "NAME_NOT_AVAILABLE" {
						name := ""
						domain := ""
						if strings.Contains(ffsEvent.Actor, "@") {
							name = strings.Split(ffsEvent.Actor, "@")[0]
							domain = strings.Split(ffsEvent.DeviceUserName, "@")[1]
						}

						user = &eventOutput.User{
							Email:  ffsEvent.Actor,
							Id:     ffsEvent.UserUid,
							Name:   name,
							Domain: domain,
						}
					} else {
						name := ""
						domain := ""
						if strings.Contains(ffsEvent.DeviceUserName, "@") {
							name = strings.Split(ffsEvent.DeviceUserName, "@")[0]
							domain = strings.Split(ffsEvent.DeviceUserName, "@")[1]
						}
						user = &eventOutput.User{
							Email:  ffsEvent.DeviceUserName,
							Id:     ffsEvent.UserUid,
							Name:   name,
							Domain: domain,
						}
					}

					if ffsEvent.OperatingSystemUser != "" && ffsEvent.OperatingSystemUser != "NAME_NOT_AVAILABLE" {
						user.Id = ffsEvent.OperatingSystemUser
					}

					if *user == (eventOutput.User{}) {
						user = nil
					}

					//host ips

					var ips []string

					if ffsEvent.PrivateIpAddresses != nil && len(ffsEvent.PrivateIpAddresses) != 0 {
						ips = append(ips, ffsEvent.PrivateIpAddresses...)
					}

					if ffsEvent.PublicIpAddress != "" {
						ips = append(ips, ffsEvent.PublicIpAddress)
					}

					if ips != nil && len(ips) == 0 {
						ips = nil
					}

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
					} else {
						geo = nil
					}

					host := &eventOutput.Host{
						Id:       ffsEvent.DeviceUid,
						Name:     ffsEvent.OsHostName,
						Hostname: ffsEvent.DomainName,
						User:     user,
						IP:       ips,
						Geo:      geo,
					}

					process := &eventOutput.Code42Process{
						Owner: ffsEvent.ProcessOwner,
						Name:  ffsEvent.ProcessName,
					}

					if *process == (eventOutput.Code42Process{}) {
						process = nil
					}

					//code 42 fields
					code42Event := &eventOutput.Code42Event{
						Id:        ffsEvent.EventId,
						Type:      ffsEvent.EventType,
						Timestamp: &eventTimestamp,
					}

					code42File := &eventOutput.Code42File{
						Path:                ffsEvent.FilePath,
						Name:                ffsEvent.FileName,
						Type:                ffsEvent.FileType,
						Category:            ffsEvent.FileCategory,
						MimeTypeByBytes:     ffsEvent.MimeTypeByBytes,
						MimeTypeByExtension: ffsEvent.MimeTypeByExtension,
						Size:                ffsEvent.FileSize,
						Owner:               ffsEvent.FileOwner,
						Hash:                hash,
						CreateTimestamp:     &createTimestamp,
						ModifyTimestamp:     &modifyTimestamp,
						Id:                  ffsEvent.FileId,
						MimeTypeMismatch:    ffsEvent.MimeTypeMismatch,
					}

					code42Device := &eventOutput.Code42Device{
						Username: ffsEvent.DeviceUserName,
						Uid:      ffsEvent.DeviceUid,
					}

					//tabs to tabs
					var tabs []eventOutput.Code42TabTab
					if ffsEvent.Tabs != nil && len(ffsEvent.Tabs) > 0 {
						for _, tab := range ffsEvent.Tabs {
							code42Tab := eventOutput.Code42TabTab{
								Title: tab.Title,
								Url:   getUrlInfo(tab.Url),
							}

							tabs = append(tabs, code42Tab)
						}
					} else {
						tabs = nil
					}

					code42 := &eventOutput.Code42{
						Event:                code42Event,
						InsertionTimestamp:   &insertionTimestamp,
						File:                 code42File,
						Device:               code42Device,
						OsHostName:           ffsEvent.OsHostName,
						DomainName:           ffsEvent.DomainName,
						PublicIpAddress:      ffsEvent.PublicIpAddress,
						PrivateIpAddresses:   ffsEvent.PrivateIpAddresses,
						Actor:                ffsEvent.Actor,
						DirectoryId:          ffsEvent.DirectoryId,
						Source:               ffsEvent.Source,
						Url:                  getUrlInfo(ffsEvent.Url),
						Shared:               ffsEvent.Shared,
						SharedWith:           ffsEvent.SharedWith,
						SharingTypeAdded:     ffsEvent.SharingTypeAdded,
						CloudDriveId:         ffsEvent.CloudDriveId,
						DetectionSourceAlias: ffsEvent.DetectionSourceAlias,
						Exposure:             ffsEvent.Exposure,
						Process:              process,
						RemovableMedia: &eventOutput.Code42RemovableMedia{
							Vendor:       ffsEvent.RemovableMediaVendor,
							Name:         ffsEvent.RemovableMediaName,
							SerialNumber: ffsEvent.RemovableMediaSerialNumber,
							Capacity:     ffsEvent.RemovableMediaCapacity,
							BusType:      ffsEvent.RemovableMediaBusType,
							MediaName:    ffsEvent.RemovableMediaMediaName,
							VolumeName:   ffsEvent.RemovableMediaVolumeName,
							PartitionId:  ffsEvent.RemovableMediaPartitionId,
						},
						SyncDestination:         ffsEvent.SyncDestination,
						SyncDestinationUsername: ffsEvent.SyncDestinationUsername,
						EmailDlp: &eventOutput.Code42EmailDlp{
							PolicyNames: ffsEvent.EmailDlpPolicyNames,
							Subject:     ffsEvent.EmailSubject,
							Sender:      ffsEvent.EmailSender,
							From:        ffsEvent.EmailFrom,
							Recipients:  ffsEvent.EmailRecipients,
						},
						OutsideActiveHours: ffsEvent.OutsideActiveHours,
						Print: &eventOutput.Code42Print{
							JobName:     ffsEvent.PrintJobName,
							PrinterName: ffsEvent.PrinterName,
						},
						RemoteActivity:      ffsEvent.RemoteActivity,
						Trusted:             ffsEvent.Trusted,
						OperatingSystemUser: ffsEvent.OperatingSystemUser,
						Destination: &eventOutput.Code42Destination{
							Category: ffsEvent.DestinationCategory,
							Name:     ffsEvent.DetectionSourceAlias,
						},
						Tabs: tabs,
					}

					elasticFileEvent := &eventOutput.ElasticFileEvent{
						Event:     event,
						Timestamp: &timestamp,
						File:      file,
						Host:      host,
						Code42:    code42,
					}

					elasticFFSEvents = append(elasticFFSEvents, *elasticFileEvent)
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
			} else if query.EsStandardized == "ecs" {
				err = eventOutput.WriteEvents(elasticFFSEvents, query)
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
				} else if query.EsStandardized == "ecs" {
					elasticWg.Add(len(elasticFFSEvents))
					go func() {
						for _, elasticFileEvent := range elasticFFSEvents {
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

							//eventTimestamp
							eventTimestamp, err := time.Parse("2006-01-02 15:04:05.000", strings.Replace(strings.Replace(ffsEvent.EventTimestamp, "T", " ", -1), "Z", "", -1))

							if err != nil {
								panic(err)
							}

							if query.Elasticsearch.IndexTimeGen == "insertionTimestamp" {
								indexTime, _ = time.Parse(query.Elasticsearch.IndexTimeAppend, eventTimestamp.Format(query.Elasticsearch.IndexTimeAppend))
							} else {
								indexTime, _ = time.Parse(query.Elasticsearch.IndexTimeAppend, eventTimestamp.Format(query.Elasticsearch.IndexTimeAppend))
							}

							requiredIndexMutex.RLock()
							if _, found := requiredIndexTimestamps[indexTime]; !found {
								requiredIndexTimestamps[indexTime] = nil
							}
							requiredIndexMutex.RUnlock()
							elasticWg.Done()
						}
					}()
				} else if query.EsStandardized == "ecs" {
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
								//eventTimestamp
								insertionTimestamp, err := time.Parse("2006-01-02 15:04:05.000", strings.Replace(strings.Replace(ffsEvent.InsertionTimestamp, "T", " ", -1), "Z", "", -1))

								if err != nil {
									panic(err)
								}

								indexTime, _ = time.Parse(query.Elasticsearch.IndexTimeAppend, insertionTimestamp.Format(query.Elasticsearch.IndexTimeAppend))
							} else {
								//eventTimestamp
								eventTimestamp, err := time.Parse("2006-01-02 15:04:05.000", strings.Replace(strings.Replace(ffsEvent.EventTimestamp, "T", " ", -1), "Z", "", -1))

								if err != nil {
									panic(err)
								}

								indexTime, _ = time.Parse(query.Elasticsearch.IndexTimeAppend, eventTimestamp.Format(query.Elasticsearch.IndexTimeAppend))
							}
							indexName := elasticsearch.BuildIndexNameWithTime(query.Elasticsearch, indexTime)
							r := elastic.NewBulkIndexRequest().Index(indexName).Doc(ffsEvent)
							processor.Add(r)
							elasticWg.Done()
						}
					}()
				} else if query.EsStandardized == "ecs" {
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
			} else if query.EsStandardized == "ecs" {
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
	} else {
		enrichmentTime = time.Now()
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
