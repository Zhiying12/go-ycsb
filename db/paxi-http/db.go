package paxihttp

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/ailidani/paxi"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"regexp"
)

type paxiHttpClient struct {
	client *Client
}

func (db *paxiHttpClient) ToSqlDB() *sql.DB {
	return nil
}

func (c *paxiHttpClient) Close() error {
	return nil
}

func (c *paxiHttpClient) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (c *paxiHttpClient) CleanupThread(_ context.Context) {
}

func (c *paxiHttpClient) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	result, err := c.client.Get(key)
	if err != nil {
		return nil, err
	}
	valueByte := []byte(result)
	fieldMap := decode(valueByte)
	return fieldMap, nil
}

func (c *paxiHttpClient) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, fmt.Errorf("scan is not supported")
}

func (c *paxiHttpClient) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	return c.Insert(ctx, table, key, values)
}

func (c *paxiHttpClient) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	valBytes := encode(values)
	val := string(valBytes[:])
	err := c.client.Put(key, val)
	if err != nil {
		return err
	}
	return nil
}

func (c *paxiHttpClient) Delete(ctx context.Context, table string, key string) error {
	return fmt.Errorf("scan is not supported")
}

type paxiHttpCreator struct{}

func (r paxiHttpCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	// Create connection
	var config paxi.Config
	config.LoadFromPath("paxi-config.json")
	client := NewClient(&config)
	mpaxos := &paxiHttpClient{
		client: client,
	}

	return mpaxos, nil
}

func init() {
	ycsb.RegisterDBCreator("paxi", paxiHttpCreator{})
}

func encode(values map[string][]byte) []byte {
	valBytes := make([]byte, 0)
	for field, val := range values {
		fieldBytes := []byte(" " + field + "=")
		valBytes = append(valBytes, fieldBytes...)
		valBytes = append(valBytes, val...)
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
