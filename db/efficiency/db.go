package multipaxos

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/magiconair/properties"
	config2 "github.com/pingcap/go-ycsb/config"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"regexp"
)

type efficiencyClient struct {
	client *Client
}

func (db *efficiencyClient) ToSqlDB() *sql.DB {
	return nil
}

func (c *efficiencyClient) Close() error {
	return nil
}

func (c *efficiencyClient) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (c *efficiencyClient) CleanupThread(_ context.Context) {
}

func (c *efficiencyClient) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	result, err := c.client.Get(key)
	if err != nil {
		return nil, err
	}
	valueByte := []byte(result)
	fieldMap := decode(valueByte)
	return fieldMap, nil
}

func (c *efficiencyClient) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, fmt.Errorf("scan is not supported")
}

func (c *efficiencyClient) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	return c.Insert(ctx, table, key, values)
}

func (c *efficiencyClient) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	valBytes := encode(values)
	val := string(valBytes[:])
	_, err := c.client.Put(key, val)
	if err != nil {
		return err
	}
	return nil
}

func (c *efficiencyClient) Delete(ctx context.Context, table string, key string) error {
	_, err := c.client.Delete(key)
	if err != nil {
		return err
	}
	return nil
}

type efficiencyCreator struct{}

func (r efficiencyCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	// Create connection
	config, err := config2.LoadDefaultConfig()
	if err != nil {
		return nil, err
	}
	client := NewClient(config)
	mpaxos := &efficiencyClient{
		client: client,
	}

	return mpaxos, nil
}

func init() {
	ycsb.RegisterDBCreator("efficiency", efficiencyCreator{})
}

func encode(values map[string][]byte) []byte {
	valBytes := make([]byte, 0)
	for field, val := range values {
		fieldBytes := []byte(field + "=")
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
