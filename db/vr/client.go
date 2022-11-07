package vr

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap/go-ycsb/config"
	"log"
	"net"
	"net/textproto"
	"time"
)

type Client struct {
	id       uint64
	lastReqId uint64
	conn     net.Conn
	servers  []string
	leaderId int
	reader   *textproto.Reader
}

func NewClient(config *config.Config) *Client {
	client := Client{
		id:       0,
		lastReqId: 1,
		servers:  config.Address,
		leaderId: config.LeaderId,
	}

	for {
		conn, err := net.Dial("udp", client.servers[client.leaderId])
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
	request := "R"+ key
	reqUnloggedMsg := RequestMessage{
		Req: &Request{
			Op:          []byte(request),
			Clientid:    &c.id,
			Clientreqid: &c.lastReqId,
		},
	}
	data, _ := proto.Marshal(&reqUnloggedMsg)
	return c.sendRequest(string(data), true)
}

func (c *Client) Put(key string, val string) (string, error) {
	request := "I" + key + val
	reqMsg := RequestMessage{
		Req:  &Request{
			Op:            []byte(request),
			Clientid:      &c.id,
			Clientreqid:   &c.lastReqId,
		},
	}
	data, _ := proto.Marshal(&reqMsg)
	return c.sendRequest(string(data), false)
}

func (c *Client) Delete(key string) (string, error) {
	request := "del " + key
	return c.sendRequest(request, false)
}

func (c *Client) Close() {
	c.conn.Close()
}

func (c *Client) sendRequest(request string, isRead bool) (string, error) {
	sendBuffer := c.serializeMessage(request, isRead)
	_, err := c.conn.Write(sendBuffer)
	if err != nil {
		return "", err
	}
	c.lastReqId += 1

	recvBuffer := make([]byte, 65536)
	_, err = c.conn.Read(recvBuffer)
	if err != nil {
		return "", err
	}
	return "", nil
}

func (c *Client) serializeMessage(data string, isRead bool) []byte {
	var requestBuffer bytes.Buffer

	const MsgType = "specpaxos.vr.proto.RequestMessage"
	const UnLogMsgType = "specpaxos.vr.proto.UnloggedRequestMessage"
	const NONFRAGMAGIC uint32 = 0x20050318
	const MetaLen uint32 = 0

	dataLen := uint64(len(data))

	numBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(numBytes, NONFRAGMAGIC)
	requestBuffer.Write(numBytes)

	numBytes = make([]byte, 4)
	binary.LittleEndian.PutUint32(numBytes, MetaLen)
	requestBuffer.Write(numBytes)

	/*if isRead {
		typeLen := uint64(len(UnLogMsgType))
		numBytes = make([]byte, 8)
		binary.LittleEndian.PutUint64(numBytes, typeLen)
		requestBuffer.Write(numBytes)
		requestBuffer.WriteString(UnLogMsgType)
	} else {*/
		typeLen := uint64(len(MsgType))
		numBytes = make([]byte, 8)
		binary.LittleEndian.PutUint64(numBytes, typeLen)
		requestBuffer.Write(numBytes)
		requestBuffer.WriteString(MsgType)
	//}

	numBytes = make([]byte, 8)
	binary.LittleEndian.PutUint64(numBytes, dataLen)
	requestBuffer.Write(numBytes)

	requestBuffer.WriteString(data)
	return requestBuffer.Bytes()
}
