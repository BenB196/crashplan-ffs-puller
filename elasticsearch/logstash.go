package elasticsearch

import (
	"net"
	"time"
)

func CreateLogstashClient(logstashURL string) (net.Conn,error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp",logstashURL)

	if err != nil {
		return nil, err
	}
	
	d := net.Dialer{
		Timeout:       30 * time.Second,
	}

	connection, err := d.Dial("tcp", tcpAddr.String())

	if err != nil {
		return nil, err
	}

	return connection, nil
}