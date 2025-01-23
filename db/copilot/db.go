package copilot

import (
	"bufio"
	"context"
	"fmt"
	"github.com/pingcap/go-ycsb/db/copilot/genericsmrproto"
	"github.com/pingcap/go-ycsb/db/copilot/masterproto"
	"github.com/pingcap/go-ycsb/db/copilot/state"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"hash/fnv"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"regexp"
	"strings"
	"time"

	"github.com/magiconair/properties"
)

// copilot properties
const (
	copilotMaster        = "copilot.master"
	copilotMasterDefault = "127.0.0.1:7087"
)

const REQUEST_TIMEOUT = 60000 * time.Millisecond
const GET_VIEW_TIMEOUT = 60000 * time.Millisecond

var defaultValue = []byte(strings.Repeat("a", 500))

type copilotCreator struct {
}

type View struct {
	ViewId    int32
	PilotId   int32
	ReplicaId int32
	Active    bool
}

type Response struct {
	OpId       int32
	rcvingTime time.Time
	timestamp  int64
	value      state.Value
}

type copilotClient struct {
	N               int
	clientId        uint32
	servers         []net.Conn
	readers         []*bufio.Reader
	writers         []*bufio.Writer
	ops             int32
	successful      []int
	views           []*View
	viewChangeChan  chan *View
	pilot0ReplyChan chan Response
	p               *properties.Properties
	masterAddress   string
}

type contextKey string

const stateKey = contextKey("copilotClient")

func generateRandomClientId() uint32 {
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)

	return r.Uint32() % 16
}

func HashStringToInt(s string, maxInt int) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32()) % maxInt
}

func (c copilotCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	client := &copilotClient{}
	client.p = p
	client.masterAddress = p.GetString(copilotMaster, copilotMasterDefault)
	client.ops = 0

	return client, nil
}

func (db *copilotClient) createTable() error {
	return nil
}

func (db *copilotClient) Close() error {
	return nil
}

func (db *copilotClient) InitThread(ctx context.Context, threadID int, _ int) context.Context {
	db.clientId = uint32(threadID)

	master, err := rpc.DialHTTP("tcp", db.masterAddress)
	if err != nil {
		log.Fatalf("Error connecting to master\n")
	}

	rlReply := new(masterproto.GetReplicaListReply)
	err = master.Call("Master.GetReplicaList", new(masterproto.GetReplicaListArgs), rlReply)
	if err != nil {
		log.Fatalf("Error making the GetReplicaList RPC")
	}

	db.N = len(rlReply.ReplicaList)
	db.servers = make([]net.Conn, db.N)
	db.readers = make([]*bufio.Reader, db.N)
	db.writers = make([]*bufio.Writer, db.N)

	for i := 0; i < db.N; i++ {
		var err error
		db.servers[i], err = net.Dial("tcp", rlReply.ReplicaList[i])
		if err != nil {
			log.Printf("Error connecting to replica %d\n", i)
		}
		db.readers[i] = bufio.NewReader(db.servers[i])
		db.writers[i] = bufio.NewWriter(db.servers[i])
	}

	fmt.Println("Registering db id", db.clientId)
	/* Register Client Id */
	for i := 0; i < db.N; i++ {
		rciArgs := &genericsmrproto.RegisterClientIdArgs{ClientId: db.clientId}
		db.writers[i].WriteByte(genericsmrproto.REGISTER_CLIENT_ID)
		rciArgs.Marshal(db.writers[i])
		db.writers[i].Flush()
	}

	time.Sleep(5 * time.Second)

	db.successful = make([]int, db.N)
	leader := -1

	// second leader
	leader2 := -1

	// two leaders
	reply := new(masterproto.GetTwoLeadersReply)

	if err = master.Call("Master.GetTwoLeaders", new(masterproto.GetTwoLeadersArgs), reply); err != nil {
		log.Fatalf("Error making the GetTwoLeaders")
	}
	leader = reply.Leader1Id
	leader2 = reply.Leader2Id
	//fmt.Printf("The leader 1 is replica %d. The leader 2 is replica %d\n", leader, leader2)
	fmt.Printf("The leader 1 is replica %d (%s). The leader 2 is replica %d (%s)\n", leader, rlReply.ReplicaList[leader], leader2, rlReply.ReplicaList[leader2])

	// Init views. Assume initial view id is 0
	db.views = make([]*View, 2)
	db.views[0] = &View{ViewId: 0, PilotId: 0, ReplicaId: int32(leader), Active: true}
	db.views[1] = &View{ViewId: 0, PilotId: 1, ReplicaId: int32(leader2), Active: true}

	db.pilot0ReplyChan = make(chan Response) //TODO: check the size here
	db.viewChangeChan = make(chan *View, 100)

	for i := 0; i < db.N; i++ {
		go db.waitRepliesPilot(i)
	}

	return context.WithValue(ctx, "tid", threadID)
}

func (db *copilotClient) CleanupThread(_ctx context.Context) {

}

func (db *copilotClient) Read(ctx context.Context, table string, key string,
	fields []string) (map[string][]byte, error) {
	val, err := db.doCopilotRequest(ctx, key, nil, false)
	if err != nil {
		returnMap := make(map[string][]byte)
		returnMap[key] = val
		return returnMap, err
	} else {
		return nil, err
	}
}

func (db *copilotClient) Scan(ctx context.Context, table string, startKey string,
	count int, fields []string) ([]map[string][]byte, error) {
	return nil, fmt.Errorf("scan is not supported")
}

func (db *copilotClient) execQuery(ctx context.Context, query string,
	args ...interface{}) error {
	return fmt.Errorf("execQuery is not supported")
}

func (db *copilotClient) Update(ctx context.Context, table string, key string,
	values map[string][]byte) error {
	_, err := db.doCopilotRequest(ctx, key, values, true)
	return err
}

func (db *copilotClient) Insert(ctx context.Context, table string, key string,
	values map[string][]byte) error {
	_, err := db.doCopilotRequest(ctx, key, values, true)
	return err
}

func (db *copilotClient) Delete(ctx context.Context, table string,
	key string) error {
	return fmt.Errorf("delete is not supported")
}

func (db *copilotClient) doCopilotRequest(ctx context.Context, key string,
	values map[string][]byte, isUpdate bool) ([]byte, error) {

	var returnErr error = nil
	returnBytes := make([]byte, 8)

	id := db.ops
	db.ops += 1

	args := genericsmrproto.Propose{id, state.Command{ClientId: db.clientId, OpId: id, Op: state.PUT}, time.Now().UnixNano()}

	args.Command.K = state.Key(key)
	if len(key) != 23 {
		log.Println("key size ", len(key))
	}

	if isUpdate {
		args.Command.Op = state.PUT
		val := encode(values)
		args.Command.V = val
	} else {
		args.Command.Op = state.GET
		args.Command.V = defaultValue
	}

	leader := -1
	leader2 := -1
	var to *time.Timer
	succeeded := false

	var pilotErr, pilotErr1 error
	var lastGVSent0, lastGVSent1 time.Time

	for {
		// Check if there is newer view
		for i := 0; i < len(db.viewChangeChan); i++ {
			newView := <-db.viewChangeChan
			if newView.ViewId > db.views[newView.PilotId].ViewId {
				fmt.Printf("New view info: pilotId %v,  ViewId %v, ReplicaId %v\n", newView.PilotId, newView.ViewId, newView.ReplicaId)
				db.views[newView.PilotId].PilotId = newView.PilotId
				db.views[newView.PilotId].ReplicaId = newView.ReplicaId
				db.views[newView.PilotId].ViewId = newView.ViewId
				db.views[newView.PilotId].Active = true
			}
		}

		// get random server to ask about new view
		serverId := rand.Intn(db.N)
		if db.views[0].Active {
			leader = int(db.views[0].ReplicaId)
			pilotErr = nil
			if leader >= 0 {
				db.writers[leader].WriteByte(genericsmrproto.PROPOSE)
				args.Marshal(db.writers[leader])
				pilotErr = db.writers[leader].Flush()
				if pilotErr != nil {
					db.views[0].Active = false
				} else {
					succeeded = true
				}
			}
		}
		if !db.views[0].Active {
			leader = -1
			if lastGVSent0 == (time.Time{}) || time.Since(lastGVSent0) >= GET_VIEW_TIMEOUT {
				for ; serverId == 0; serverId = rand.Intn(db.N) {
				}
				getViewArgs := &genericsmrproto.GetView{0}
				db.writers[serverId].WriteByte(genericsmrproto.GET_VIEW)
				getViewArgs.Marshal(db.writers[serverId])
				db.writers[serverId].Flush()
				lastGVSent0 = time.Now()
			}
		}

		if db.views[1].Active {
			leader2 = int(db.views[1].ReplicaId)
			/* Send to second leader for two-leader protocol */
			pilotErr1 = nil
			if leader2 >= 0 {
				db.writers[leader2].WriteByte(genericsmrproto.PROPOSE)
				args.Marshal(db.writers[leader2])
				pilotErr1 = db.writers[leader2].Flush()
				if pilotErr1 != nil {
					db.views[1].Active = false
				} else {
					succeeded = true
				}
			}
		}
		if !db.views[1].Active {
			leader2 = -1
			if lastGVSent1 == (time.Time{}) || time.Since(lastGVSent1) >= GET_VIEW_TIMEOUT {
				for ; serverId == 1; serverId = rand.Intn(db.N) {
				}
				getViewArgs := &genericsmrproto.GetView{1}
				db.writers[serverId].WriteByte(genericsmrproto.GET_VIEW)
				getViewArgs.Marshal(db.writers[serverId])
				db.writers[serverId].Flush()
				lastGVSent1 = time.Now()
			}
		}
		if !succeeded {
			continue
		}

		// we successfully sent to at least one pilot
		succeeded = false
		to = time.NewTimer(REQUEST_TIMEOUT)
		toFired := false
		for true {
			select {
			case e := <-db.pilot0ReplyChan:
				repliedCmdId := e.OpId
				if !isUpdate {
					returnBytes = e.value
				}
				//batch = e.timestamp
				//rcvingTime = e.rcvingTime
				if repliedCmdId == id {
					to.Stop()
					succeeded = true
				}

			case <-to.C:
				//fmt.Printf("Client %v: TIMEOUT for request %v\n", db.clientId, id)
				//rcvingTime = time.Now()
				succeeded = false
				toFired = true

			default:
			}

			if succeeded {
				break
			} else if toFired {
				returnErr = fmt.Errorf("Client %v: TIMEOUT for request %v\n", db.clientId, id)
				break
			}

			/*if repliedCmdId != -1 && repliedCmdId < id {
				// update latency if this response actually arrived ealier
				newLat := int64(rcvingTime.Sub(timestamps[repliedCmdId]) / time.Microsecond)
				if newLat < latencies[repliedCmdId] {
					latencies[repliedCmdId] = newLat
				}
			}*/
		} // end of for loop waiting for result
		// successfully get the response. continue with the next request
		if succeeded {
			break
		} else if toFired {
			continue
		}
	}

	return returnBytes, returnErr
}

func (db *copilotClient) waitRepliesPilot(leader int) {

	var msgType byte
	var err error

	reply := new(genericsmrproto.ProposeReplyTS)
	getViewReply := new(genericsmrproto.GetViewReply)
	for true {
		if msgType, err = db.readers[leader].ReadByte(); err != nil {
			break
		}

		switch msgType {
		case genericsmrproto.PROPOSE_REPLY:
			if err = reply.Unmarshal(db.readers[leader]); err != nil {
				break
			}
			if reply.OK != 0 {
				db.successful[leader]++
				db.pilot0ReplyChan <- Response{reply.CommandId, time.Now(), reply.Timestamp, reply.Value}
				/*if expected == c.successful[leader] {
					return
				}*/
			}
			break

		case genericsmrproto.GET_VIEW_REPLY:
			if err = getViewReply.Unmarshal(db.readers[leader]); err != nil {
				break
			}
			if getViewReply.OK != 0 { /*View is active*/
				db.viewChangeChan <- &View{getViewReply.ViewId, getViewReply.PilotId, getViewReply.ReplicaId, true}
			}
			break

		default:
			break
		}
	}

}

func init() {
	ycsb.RegisterDBCreator("copilot", copilotCreator{})
}

func encode(values map[string][]byte) []byte {
	valBytes := make([]byte, 0)
	for _, val := range values {
		//fieldBytes := []byte(field + "=")
		//valBytes = append(valBytes, fieldBytes...)
		valBytes = append(valBytes, val...)
	}
	if len(valBytes) != 500 {
		log.Println("val size ", len(valBytes))
		log.Println(string(valBytes))
	}
	return valBytes
}

func decode(values []byte) map[string][]byte {
	// Assume the field name is from "field0" to "field9"
	m := regexp.MustCompile("field[0-9]=")
	valString := string(values)
	fields := m.FindAllString(valString, -1)
	vals := m.Split(valString, -1)

	fieldMap := make(map[string][]byte)
	for i, val := range vals {
		// the first val is nil
		if i == 0 {
			continue
		}
		fieldMap[fields[i-1]] = []byte(val)
	}
	return fieldMap
}
