package paxihttp

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/paxos"
)

type Client struct {
	conn     paxi.Client
	servers  []string
	leaderId paxi.ID
}

func NewClient(config *paxi.Config) *Client {
	client := Client{
		servers: make([]string, 0, len(config.HTTPAddrs)),
		leaderId: paxi.NewID(1, 1),
	}

	for _, addr := range config.Addrs {
		client.servers = append(client.servers, addr)
	}

	client.conn = paxos.NewClient(paxi.ID(client.leaderId), config)
	return &client
}

func (c *Client) Get(key string) (paxi.Value, error) {
	value, err := c.conn.Get(paxi.Key(key))
	if err != nil {
		return []byte{}, err
	}
	return value, nil
}

func (c *Client) Put(key string, val string) error {
	c.conn.Put(paxi.Key(key), paxi.Value(val))
	return nil 
}
