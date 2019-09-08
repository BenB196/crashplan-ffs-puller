package main

import (
	"flag"
	"github.com/BenB196/crashplan-ffs-puller/config"
	"github.com/BenB196/crashplan-ffs-puller/ffsEvent"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"net/http"
	"strconv"
	"sync"
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
	wg.Add(len(configuration.FFSQueries))
	go func() {
		for _, query := range configuration.FFSQueries {
			go ffsEvent.FFSQuery(configuration, query, wg)
		}
	}()

	if configuration.Prometheus.Enabled {
		//startup prometheus metrics port
		http.Handle("/metrics",promhttp.Handler())

		log.Fatal(http.ListenAndServe(":" + strconv.Itoa(configuration.Prometheus.Port), nil))
	}

	wg.Wait()
}