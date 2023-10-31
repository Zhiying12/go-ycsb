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

	for {
		conn, err := net.Dial("tcp", client.servers[client.leaderId])
		if err != nil {
			log.Printf("conn err: %v\n", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		client.conn = conn
		client.reader = textproto.NewReader(bufio.NewReader(conn))
		break
	}
	return &client
}

func (c *Client) Get(key string) (string, error) {
	request := "get "+ key + "\n"
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
	sendBuffer := []byte(request)
	_, err := c.conn.Write(sendBuffer)
	if err != nil {
		return "", err
	}
	//return "", err

	result, err := c.reader.ReadLine()
	if err != nil {
		//log.Println(err)
		return "", err
	}
	if result == "retry" || result == "leader is ..." || result == "bad command" {
		//log.Println(result)
		return "", errors.New(result)
	}
	return result, nil
}
