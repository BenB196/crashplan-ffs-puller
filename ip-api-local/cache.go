package ip_api_local

import (
	"encoding/json"
	"github.com/BenB196/crashplan-ffs-puller/promMetrics"
	"github.com/BenB196/ip-api-go-pkg"
	"github.com/VictoriaMetrics/fastcache"
	"log"
	"strings"
	"time"
)

type Record struct {
	ExpirationTime 	time.Time		`json:"expirationTime"`
	Location		ip_api.Location	`json:"location"`
}

var FastCacheCache = fastcache.New(32000000)

/*
GetLocation - function for getting the location of a query from cache
query - IP/DNS entry
fields - string of comma separated values
returns
ip_api Location
error
*/
func GetLocation(query string, fields string) (*ip_api.Location, bool, error) {
	//Check if cache has anything in it, skip if not
	if FastCacheCache == nil {
		//record not found in cache return false
		return nil, false, nil
	}

	//Set timezone to UTC
	loc, _ := time.LoadLocation("UTC")
	queryBytes := []byte(query)
	//Check if record exists in cache
	if recordBytes, found := FastCacheCache.HasGet(nil, queryBytes); found {
		//convert record bytes to record
		var record Record
		err := json.Unmarshal(recordBytes, &record)

		if err != nil {
			return nil, false, err
		}
		//Check if record has not expired
		if time.Now().In(loc).Sub(record.ExpirationTime) > 0 {
			//Remove record if expired and return false
			promMetrics.DecreaseQueriesCachedCurrent()
			FastCacheCache.Del(queryBytes)
			return nil, false, nil
		}

		location := ip_api.Location{}

		//Set default fields if fields string is empty
		if fields == "" {
			fields = "query,status,country,countryCode,region,regionName,city,zip,lat,lon,timezone,isp,org,as"
		}

		//check if all fields are passed, if so just return location
		if len(fields) == len(ip_api.AllowedAPIFields) {
			return &record.Location, true, nil
		} else {
			fieldSlice := strings.Split(fields,",")
			//Loop through fields and set selected fields
			for _, field := range fieldSlice {
				switch field {
				case "status":
					location.Status = record.Location.Status
				case "message":
					location.Message = record.Location.Message
				case "continent":
					location.Continent = record.Location.Continent
				case "continentCode":
					location.ContinentCode = record.Location.ContinentCode
				case "country":
					location.Country = record.Location.Country
				case "countryCode":
					location.CountryCode = record.Location.CountryCode
				case "region":
					location.Region = record.Location.Region
				case "regionName":
					location.RegionName = record.Location.RegionName
				case "city":
					location.City = record.Location.City
				case "district":
					location.District = record.Location.District
				case "zip":
					location.ZIP = record.Location.ZIP
				case "lat":
					location.Lat = record.Location.Lat
				case "lon":
					location.Lon = record.Location.Lon
				case "timezone":
					location.Timezone = record.Location.Timezone
				case "isp":
					location.ISP = record.Location.ISP
				case "org":
					location.Org = record.Location.Org
				case "as":
					location.AS = record.Location.AS
				case "asname":
					location.ASName = record.Location.ASName
				case "reverse":
					location.Reverse = record.Location.Reverse
				case "mobile":
					location.Mobile = record.Location.Mobile
				case "proxy":
					location.Proxy = record.Location.Proxy
				case "hosting":
					location.Hosting = record.Location.Hosting
				case "query":
					location.Query = record.Location.Query
				}
			}
		}
		//Return location
		return &location, true, nil
	}
	//record not found in cache return false
	return nil, false, nil
}

/*
AddLocation - adds a query + location to cache map along with an expiration time
query - IP/DNS value
location - ip_api location
expirationDuration - duration in which the query will expire (go stale)
*/
func AddLocation(query string, location ip_api.Location, expirationDuration time.Duration) (bool, error) {
	//Set timezone to UTC
	loc, _ := time.LoadLocation("UTC")

	//Get expiration time
	expirationTime := time.Now().In(loc).Add(expirationDuration)

	//marshal record
	record := Record{
		ExpirationTime: expirationTime,
		Location:       location,
	}
	locationBytes, err := json.Marshal(record)

	if err != nil {
		return false, err
	}

	//Create and Add record to cache
	FastCacheCache.Set([]byte(query), locationBytes)

	promMetrics.IncrementQueriesCachedTotal()
	promMetrics.IncrementQueriesCachedCurrent()

	return true, nil
}

/*
WriteCache - writes the cache to a file on disk to be read on app restarts
writeLocation - string containing the write path
*/
func WriteCache(writeLocation *string) {
	log.Println("Starting Cache Write")
	//create file name
	fileName := *writeLocation + "ip_api_proxy_cache.gob"

	err := FastCacheCache.SaveToFile(fileName)

	if err != nil {
		panic(err)
	}

	log.Println("Finished Cache Write")
}

/*
ReadCache - reads the cache file from disk and loads it into the Cache map
writeLocation - string containing the file path.
*/
func ReadCache(writeLocation *string) {
	//create filename
	fileName := *writeLocation + "ip_api_proxy_cache.gob"

	FastCacheCache = fastcache.LoadFromFileOrNew(fileName, 32000000)
}