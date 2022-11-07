package paxi

import (
	"encoding/gob"
	"github.com/ailidani/paxi"
	"log"
	"net"
	"time"
)

type Client struct {
	conn     net.Conn
	servers  []string
	leaderId int
	writer   *gob.Encoder
	reader   *gob.Decoder
	ownAddr  string
}

func NewClient(config *Config) *Client {
	client := Client{
		servers: make([]string, 0, len(config.Addrs)),
		leaderId: config.PrefId,
	}

	for _, addr := range config.Addrs {
		client.servers = append(client.servers, addr)
	}

	for {
		conn, err := net.Dial("tcp", client.servers[client.leaderId])
		if err != nil {
			log.Printf("conn err: %v\n", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		client.conn = conn
		client.ownAddr = conn.LocalAddr().String()
		client.reader = gob.NewDecoder(conn)
		client.writer = gob.NewEncoder(conn)
		break
	}
	return &client
}

func (c *Client) Get(key string) (paxi.Value, error) {
	cmd := paxi.Command{
		Key:         paxi.Key(key),
		Value:       []byte{},
	}
	request := paxi.Request{
		Command:    cmd,
		Properties: make(map[string]string),
		Timestamp:  time.Now().UnixNano(),
		NodeID:     "",
	}
	return c.sendRequest(request)
}

func (c *Client) Put(key string, val string) (paxi.Value, error) {
	cmd := paxi.Command{
		Key:         paxi.Key(key),
		Value:       []byte(val),
	}
	request := paxi.Request{
		Command:    cmd,
		Properties: make(map[string]string),
		Timestamp:  time.Now().UnixNano(),
		NodeID:     "",
	}
	return c.sendRequest(request)
}

func (c *Client) Close() {
	c.conn.Close()
}

func (c *Client) sendRequest(request interface{}) (paxi.Value, error) {
	err := c.writer.Encode(&request)
	if err != nil {
		return []byte{}, err
	}

	var reply paxi.Reply
	err = c.reader.Decode(&reply)
	if err != nil {
		return []byte{}, err
	}
	return reply.Value, nil
}
