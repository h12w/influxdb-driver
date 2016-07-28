package influxdb

import (
	"bytes"
	"fmt"
	"net"
	"time"
)

// UDPPayloadSize is a reasonable default payload size for UDP packets that
// could be travelling over the internet.
const (
	UDPPayloadSize = 512
)

// UDPConfig is the config data needed to create a UDP Client
type UDPConfig struct {
	// Addr should be of the form "host:port"
	// or "[ipv6-host%zone]:port".
	Addr string

	// PayloadSize is the maximum size of a UDP client message, optional
	// Tune this based on your network. Defaults to UDPBufferSize.
	PayloadSize int
}

// NewUDPClient returns a client interface for writing to an InfluxDB UDP
// service from the given config.
func NewUDPClient(conf UDPConfig) (Client, error) {
	var udpAddr *net.UDPAddr
	udpAddr, err := net.ResolveUDPAddr("udp", conf.Addr)
	if err != nil {
		return nil, err
	}

	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return nil, err
	}

	payloadSize := conf.PayloadSize
	if payloadSize == 0 {
		payloadSize = UDPPayloadSize
	}

	return &udpclient{
		conn:        conn,
		payloadSize: payloadSize,
	}, nil
}

type udpclient struct {
	conn        *net.UDPConn
	payloadSize int
}

func (uc *udpclient) Query(q Query) (*Response, error) {
	return nil, fmt.Errorf("Querying via UDP is not supported")
}

// Ping will check to see if the server is up with an optional timeout on waiting for leader.
// Ping returns how long the request took, the version of the server it connected to, and an error if one occurred.
func (uc *udpclient) Ping(timeout time.Duration) (time.Duration, string, error) {
	return 0, "", nil
}

// Close releases the udpclient's resources.
func (uc *udpclient) Close() error {
	return uc.conn.Close()
}
func (uc *udpclient) Write(lineData []byte, cfg *WriteConfig) error {
	return udpWrite(uc.conn, lineData, uc.payloadSize)
}

func udpWrite(conn net.Conn, data []byte, size int) error {
	head, tail := []byte(nil), data
	for {
		head, tail = udpPayload(tail, size)
		if len(head) == 0 {
			break
		}
		_, err := conn.Write(data)
		if err != nil {
			return err
		}
	}
	return nil
}
func udpPayload(data []byte, size int) (head, tail []byte) {
	head, tail = nil, data
	for len(head) < size {
		if pos := bytes.IndexByte(tail, '\n'); pos > 0 {
			head, tail = append(head, tail[:pos]...), tail[pos:]
			continue
		}
		break
	}
	if head == nil {
		head, tail = data, nil
	}
	return head, tail
}
