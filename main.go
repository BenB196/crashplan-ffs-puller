package main

import (
	"crashplan-ffs-puller/config"
	"crashplan-ffs-puller/ffsEvent"
	"flag"
	"log"
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {

	//Get Config location and get config struct
	var configLocation string
	flag.StringVar(&configLocation,"config","","Configuation File Location. REQUIRED") //TODO improve usage description
	var port = flag.String("listening port",":8080", "The port to listen on for HTTP requests.")


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
	go func() {
		for _, query := range configuration.FFSQueries {
			wg.Add(1)
			go ffsEvent.FFSQuery(configuration, query, wg)
			defer wg.Done()
		}
	}()

	wg.Wait()

	//startup prometheus metrics port
	http.Handle("/metrics",promhttp.Handler())

	log.Fatal(http.ListenAndServe(*port, nil))
}