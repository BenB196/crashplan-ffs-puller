package main

import (
	"crashplan-ffs-puller/config"
	"crashplan-ffs-puller/ffsEvent"
	"flag"
	"log"
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
	for _, query := range configuration.FFSQueries {
		wg.Add(1)
		go ffsEvent.FFSQuery(configuration, query, wg)
	}

	wg.Wait()
}