package main

import (
	config "crashplan-ffs-puller/config"
	"flag"
	"log"
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

	//Quick lazy test
	log.Println(configuration.AuthURI)
	log.Println(configuration.FFSURI)
	for _, query := range configuration.FFSQueries {
		log.Println(query.Username)
		log.Println(query.Password)
		log.Println(query.QueryInterval)
		log.Println(query.Query)
	}
}