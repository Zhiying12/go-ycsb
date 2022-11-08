package multipaxos

import (
	"bufio"
	"github.com/pingcap/go-ycsb/config"
	"github.com/pingcap/go-ycsb/db/efficiency/genericsmrproto"
	"github.com/pingcap/go-ycsb/db/efficiency/state"
	"log"
	"net"
	"time"
)

type Client struct {
	servers  []string
	leaderId int
	reader   *bufio.Reader
	writer   *bufio.Writer
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
		client.reader = bufio.NewReader(conn)
		client.writer = bufio.NewWriter(conn)
		break
	}
	return &client
}

func (c *Client) Get(key string) (string, error) {
	var value []byte
	for i := 0; i < 1070; i++ {
		value = append(value, []byte("0")[0])
	}

	request := genericsmrproto.Propose{
		CommandId: 0,
		Command: state.Command{
			Op: state.GET,
			K:  state.Key(key),
			V:  state.Value(value),
		},
		Timestamp: 0,
	}
	return c.sendRequest(request)
}

func (c *Client) Put(key string, val string) (string, error) {
	request := genericsmrproto.Propose{
		CommandId: 0,
		Command: state.Command{
			Op: state.PUT,
			K:  state.Key(key),
			V:  state.Value(val),
		},
		Timestamp: 0,
	}
	return c.sendRequest(request)
}

func (c *Client) Delete(key string) (string, error) {
	request := genericsmrproto.Propose{
		CommandId: 0,
		Command: state.Command{
			Op: state.DELETE,
			K:  state.Key(key),
			V:  state.Value(key),
		},
		Timestamp: 0,
	}
	return c.sendRequest(request)
}

func (c *Client) sendRequest(request genericsmrproto.Propose) (string, error) {
	err := c.writer.WriteByte(genericsmrproto.PROPOSE)
	if err != nil {
		log.Println(err)
		return "", err
	}
	request.Marshal(c.writer)
	c.writer.Flush()

	reply := new(genericsmrproto.ProposeReplyTS)
	err = reply.Unmarshal(c.reader)
	if err != nil {
		log.Printf("reply err: %v", err)
		return "", err
	}
	return string(reply.Value), nil
}
