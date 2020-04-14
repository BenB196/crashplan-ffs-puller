package elasticsearch

import (
	"log"
	"math/rand"
	"net"
	"time"
)

func CreateLogstashClient(logstashURL []string) (net.Conn,error) {


	tcpAddr, err := net.ResolveTCPAddr("tcp",Balance(logstashURL))

	if err != nil {
		log.Println("Error on logstash ResolveTCPAddr: " + err.Error())
		return nil, err
	}
	
	d := net.Dialer{
		Timeout:       5 * time.Minute,
	}

	connection, err := d.Dial("tcp", tcpAddr.String())

	if err != nil {
		log.Println("Error on logstash TCP: " + err.Error())
		return nil, err
	}

	return connection, nil
}

func Balance(urls []string) string {
	return urls[rand.Intn(len(urls))]
}