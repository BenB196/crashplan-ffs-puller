package ffsEvent

import (
	"context"
	"fmt"
	"github.com/BenB196/crashplan-ffs-go-pkg"
	"github.com/BenB196/crashplan-ffs-puller/config"
	"github.com/BenB196/crashplan-ffs-puller/elasticsearch"
	"github.com/BenB196/crashplan-ffs-puller/eventOutput"
	"github.com/olivere/elastic/v7"
	"log"
	"sync"
	"time"
)

func FFSQuery(configuration config.Config, query config.FFSQuery) {

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
	authData, err := ffs.GetAuthData(configuration.AuthURI, query.Username, query.Password)

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
			case <-authTimeTicker.C:
				authData, err = ffs.GetAuthData(configuration.AuthURI, query.Username, query.Password)

				if err != nil {
					log.Println("error with getting authentication data for ffs query: " + query.Name)
					panic(err)
				}
			case <-quit:
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
		info, code, err := elasticClient.Ping(elasticsearch.Balance(query.Elasticsearch.ElasticURL)).Do(ctx)

		if err != nil {
			//TODO handle error
			log.Println("error reaching elastic server")
			panic(err)
		}

		fmt.Printf("Elasticsearch returned with code %d and version %s\n", code, info.Version.Number)
	}

	//Handle old in progress queries that never completed when programmed died
	if inProgressQueries != nil && len(inProgressQueries) > 0 {
		go func() {
			for _, inProgressQuery := range inProgressQueries {
				query = setOnOrBeforeAndAfter(query, inProgressQuery.OnOrBefore, inProgressQuery.OnOrAfter)
				queryFetcher(query, &inProgressQueries, *authData, configuration, &lastCompletedQuery, maxTime, true, elasticClient, ctx, nil, 0, false)
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
			case <-queryIntervalTimeTicker.C:
				if *query.MaxConcurrentQueries == -1 || len(inProgressQueries) <= *query.MaxConcurrentQueries {
					go queryFetcher(query, &inProgressQueries, *authData, configuration, &lastCompletedQuery, maxTime, false, elasticClient, ctx, quit, 0, false)
				} else {
					log.Println("Rate limiting query: " + query.Name)
				}
			case <-quit:
				queryIntervalTimeTicker.Stop()
				wgQuery.Done()
				return
			}
		}
	}()
	wgQuery.Wait()
	return
}
