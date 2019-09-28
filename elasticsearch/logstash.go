package elasticsearch

import (
	"net"
)

func CreateLogstashClient(logstashURL string) (net.Conn,error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp",logstashURL)

	if err != nil {
		return nil, err
	}

	connection, err := net.DialTCP("tcp",nil,tcpAddr)

	if err != nil {
		return nil, err
	}
	err = connection.SetWriteBuffer(100000)

	if err != nil {
		return nil, err
	}

	return connection, nil
}