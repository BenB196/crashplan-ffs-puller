package ffsEvent

import (
	"github.com/BenB196/crashplan-ffs-puller/config"
	"github.com/BenB196/crashplan-ffs-puller/ip-api-local"
	"github.com/BenB196/crashplan-ffs-puller/promMetrics"
	"github.com/BenB196/ip-api-go-pkg"
	"log"
)

func getIpApiLocation(configuration config.Config, publicIpAddress string) *ip_api.Location {
	var location *ip_api.Location

	//Check cache for ip
	location, found, err := ip_api_local.GetLocation(publicIpAddress + configuration.IPAPI.Lang,configuration.IPAPI.Fields)

	if err != nil {
		panic(err)
	}

	if found {
		promMetrics.IncrementHandlerRequests("200")
		promMetrics.IncrementCacheHits()
		promMetrics.IncrementSuccessfulQueries()
		promMetrics.IncrementSuccessfulSingeQueries()
		if configuration.Debugging {
			log.Println("Found: " + publicIpAddress + " in cache.")
		}
		return location
	}

	ipApiQuery := ip_api.Query{
		Queries: []ip_api.QueryIP{{Query:  publicIpAddress}},
		Fields: configuration.IPAPI.Fields,
		Lang:   configuration.IPAPI.Lang,
	}

	promMetrics.IncrementRequestsForwarded()
	promMetrics.IncrementQueriesForwarded()
	location, err = ip_api.SingleQuery(ipApiQuery, configuration.IPAPI.APIKey, configuration.IPAPI.URL, configuration.Debugging)

	if err != nil {
		if location == nil {
			location = &ip_api.Location{}
		}
		location.Status = "fail"
		location.Message = err.Error()
		promMetrics.IncrementHandlerRequests("400")
		promMetrics.IncrementFailedRequests()
		promMetrics.IncrementFailedSingleRequests()
		_, err = ip_api_local.AddLocation(publicIpAddress + configuration.IPAPI.Lang,*location,*configuration.IPAPI.LocalCache.FailedAgeDuration)
		if err != nil {
			log.Println(err)
		}
		//Re-get request with specified fields
		location, _, err = ip_api_local.GetLocation(publicIpAddress + configuration.IPAPI.Lang,configuration.IPAPI.Fields)
		if err != nil {
			if configuration.Debugging {
				log.Println("Failed single request: " + err.Error())
			}
			log.Println(err)
		}
		return location
	} else if location.Status == "success" {
		if configuration.Debugging {
			log.Println("Added: " + publicIpAddress + configuration.IPAPI.Lang + " to cache.")
		}
		promMetrics.IncrementHandlerRequests("200")
		_, err = ip_api_local.AddLocation(publicIpAddress + configuration.IPAPI.Lang,*location,*configuration.IPAPI.LocalCache.SuccessAgeDuration)
		if err != nil {
			log.Println(err)
		}
		//Re-get request with specified fields
		location, _, err = ip_api_local.GetLocation(publicIpAddress + configuration.IPAPI.Lang,configuration.IPAPI.Fields)
		if err != nil {
			log.Println(err)
		}
		promMetrics.IncrementSuccessfulQueries()
		promMetrics.IncrementSuccessfulSingeQueries()
		return location
	} else if location.Status == "fail" {
		if configuration.Debugging {
			log.Println("Failed single query: " + publicIpAddress)
		}
		promMetrics.IncrementHandlerRequests("400")
		promMetrics.IncrementFailedQueries()
		promMetrics.IncrementFailedSingleQueries()
		_, err = ip_api_local.AddLocation(publicIpAddress + configuration.IPAPI.Lang,*location,*configuration.IPAPI.LocalCache.FailedAgeDuration)
		if err != nil {
			log.Println(err)
		}
		//Re-get request with specified fields
		location, _, err = ip_api_local.GetLocation(publicIpAddress + configuration.IPAPI.Lang,configuration.IPAPI.Fields)
		if err != nil {
			log.Println(err)
		}
		return location
	} else {
		return nil
	}
}
