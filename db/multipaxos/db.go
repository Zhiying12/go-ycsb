package multipaxos

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/magiconair/properties"
	config2 "github.com/pingcap/go-ycsb/config"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"regexp"
)

const KeyNotFound = "key not found"

type mpaxosClient struct {
	client *Client
}

func (db *mpaxosClient) ToSqlDB() *sql.DB {
	return nil
}

func (c *mpaxosClient) Close() error {
	return nil
}

func (c *mpaxosClient) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (c *mpaxosClient) CleanupThread(_ context.Context) {
}

func (c *mpaxosClient) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	result, err := c.client.Get(key)
	if err != nil {
		return nil, err
	}
	if result == KeyNotFound {
		return map[string][]byte{"field0": []byte("")}, nil
	}
	valueByte := []byte(result)
	fieldMap := map[string][]byte{
		"field0": valueByte,
	}
	return fieldMap, nil
}

func (c *mpaxosClient) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, fmt.Errorf("scan is not supported")
}

func (c *mpaxosClient) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	return c.Insert(ctx, table, key, values)
}

func (c *mpaxosClient) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	valBytes := encode(values)
	val := string(valBytes[:])
	result, err := c.client.Put(key, val)
	if err != nil {
		return err
	}
	if result != "" {
		return errors.New(result)
	}
	return nil
}

func (c *mpaxosClient) Delete(ctx context.Context, table string, key string) error {
	result, err := c.client.Delete(key)
	if err != nil {
		return err
	}
	if result == KeyNotFound {
		return errors.New(KeyNotFound)
	}
	return nil
}

type mpaxosCreator struct{}

func (r mpaxosCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	// Create connection
	config, err := config2.LoadDefaultConfig()
	if err != nil {
		return nil, err
	}
	client := NewClient(config)
	mpaxos := &mpaxosClient{
		client: client,
	}

	return mpaxos, nil
}

func init() {
	ycsb.RegisterDBCreator("multipaxos", mpaxosCreator{})
}

func encode(values map[string][]byte) []byte {
	valBytes := make([]byte, 0)
	for _, val := range values {
		//fieldBytes := []byte(field + "=")
		//valBytes = append(valBytes, fieldBytes...)
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
