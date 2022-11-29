package multipaxos

import (
	"bufio"
	"errors"
	"github.com/pingcap/go-ycsb/config"
	"log"
	"net"
	"net/textproto"
	"time"
)

type Client struct {
	conn     net.Conn
	servers  []string
	leaderId int
	reader   *textproto.Reader
}

func NewClient(config *config.Config) *Client {
	client := Client{
		servers:  config.Address,
		leaderId: config.LeaderId,
	}

	return &client
}

func (c *Client) Get(key string) (string, error) {
	request := "get " + key + "\n"
	return c.sendRequest(request)
}

func (c *Client) Put(key string, val string) (string, error) {
	request := "put " + key + " " + val + "\n"
	return c.sendRequest(request)
}

func (c *Client) Delete(key string) (string, error) {
	request := "del " + key + "\n"
	return c.sendRequest(request)
}

func (c *Client) Close() {
	c.conn.Close()
}

func (c *Client) sendRequest(request string) (string, error) {
	conn, err := net.Dial("tcp", c.servers[c.leaderId])
	if err != nil {
		log.Printf("conn err: %v\n", err)
		time.Sleep(500 * time.Millisecond)
		return "", err
	}
	reader := textproto.NewReader(bufio.NewReader(conn))

	sendBuffer := []byte(request)
	_, err = conn.Write(sendBuffer)
	if err != nil {
		conn.Close()
		return "", err
	}

	result, err := reader.ReadLine()
	if err != nil {
		conn.Close()
		return "", err
	}
	if result == "retry" || result == "leader is ..." || result == "bad command" {
		conn.Close()
		return "", errors.New(result)
	}
	conn.Close()
	return result, nil
}
