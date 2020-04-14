package main

import (
	"flag"
	"github.com/BenB196/crashplan-ffs-puller/config"
	"github.com/BenB196/crashplan-ffs-puller/ffsEvent"
	"github.com/BenB196/crashplan-ffs-puller/ip-api-local"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"net/http"
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

	if configuration.IPAPI.Enabled && configuration.IPAPI.LocalCache.Enabled &&
		configuration.IPAPI.LocalCache.Persist {
		//read ip-api proxy if enabled
		ip_api_local.ReadCache(&configuration.IPAPI.LocalCache.WriteLocation)

		//write ip-api proxy if enabled
		writeIpApiTimeTicker := time.NewTicker(*configuration.IPAPI.LocalCache.WriteIntervalDuration)
		go func() {
			for {
				select {
				case <-writeIpApiTimeTicker.C:
					ip_api_local.WriteCache(&configuration.IPAPI.LocalCache.WriteLocation)
				}
			}
		}()
	}



	//Spawn goroutines for each ffs query provided
	var wg sync.WaitGroup
	wg.Add(len(configuration.FFSQueries))
	go func() {
		for _, query := range configuration.FFSQueries {
			go ffsEvent.FFSQuery(*configuration, query)
			wg.Done()
		}
	}()

	if configuration.Prometheus.Enabled {
		//startup prometheus metrics port
		http.Handle("/metrics",promhttp.Handler())

		log.Fatal(http.ListenAndServe(":" + strconv.Itoa(configuration.Prometheus.Port), nil))
	}

	wg.Wait()
}