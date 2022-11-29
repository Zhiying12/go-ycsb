// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"context"
	"fmt"
	"github.com/pingcap/go-ycsb/pkg/util"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/measurement"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type worker struct {
	p               *properties.Properties
	workerDB        ycsb.DB
	workload        ycsb.Workload
	doTransactions  bool
	doBatch         bool
	batchSize       int
	opCount         int64
	targetOpsPerMs  float64
	threadID        int
	targetOpsTickNs int64
	opsDone         int64
	wg              sync.WaitGroup
}

func newWorker(p *properties.Properties, threadID int, threadCount int, workload ycsb.Workload, db ycsb.DB) *worker {
	w := new(worker)
	w.p = p
	w.doTransactions = p.GetBool(prop.DoTransactions, true)
	w.batchSize = p.GetInt(prop.BatchSize, prop.DefaultBatchSize)
	if w.batchSize > 1 {
		w.doBatch = true
	}
	w.threadID = threadID
	w.workload = workload
	w.workerDB = db

	var totalOpCount int64
	if w.doTransactions {
		totalOpCount = p.GetInt64(prop.OperationCount, 0)
	} else {
		if _, ok := p.Get(prop.InsertCount); ok {
			totalOpCount = p.GetInt64(prop.InsertCount, 0)
		} else {
			totalOpCount = p.GetInt64(prop.RecordCount, 0)
		}
	}

	if totalOpCount < int64(threadCount) {
		fmt.Printf("totalOpCount(%s/%s/%s): %d should be bigger than threadCount: %d",
			prop.OperationCount,
			prop.InsertCount,
			prop.RecordCount,
			totalOpCount,
			threadCount)

		os.Exit(-1)
	}

	w.opCount = totalOpCount / int64(threadCount)

	targetPerThreadPerms := float64(-1)
	if v := p.GetInt64(prop.Target, 0); v > 0 {
		targetPerThread := float64(v) / float64(threadCount)
		targetPerThreadPerms = targetPerThread / 1000.0
	}

	if targetPerThreadPerms > 0 {
		w.targetOpsPerMs = targetPerThreadPerms
		w.targetOpsTickNs = int64(1000000.0 / w.targetOpsPerMs)
	}

	return w
}

func (w *worker) throttle(ctx context.Context, startTime time.Time) {
	if w.targetOpsPerMs <= 0 {
		return
	}

	d := time.Duration(w.opsDone * w.targetOpsTickNs)
	d = startTime.Add(d).Sub(time.Now())
	if d < 0 {
		return
	}
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

func (w *worker) run(ctx context.Context) {
	// spread the thread operation out so they don't all hit the DB at the same time
	if w.targetOpsPerMs > 0.0 && w.targetOpsPerMs <= 1.0 {
		time.Sleep(time.Duration(rand.Int63n(w.targetOpsTickNs)))
	}

	startTime := time.Now()
	executionTime := w.p.GetInt64(prop.MaxExecutiontime, 0)

	for w.opCount == 0 || w.opsDone < w.opCount {
		opsCount := 1
		w.wg.Add(1)
		go w.Do(ctx)

		if measurement.IsWarmUpFinished() {
			w.opsDone += int64(opsCount)
			w.throttle(ctx, startTime)
		}

		if executionTime != 0 {
			if time.Now().Sub(startTime) > time.Duration(executionTime)*time.Second {
				return
			}
		}

		select {
		case <-ctx.Done():
			return
		default:
		}
	}
	w.wg.Wait()
}

func (w *worker) Do(ctx context.Context) {
	var err error
	if w.doTransactions {
		err = w.workload.DoTransaction(ctx, w.workerDB)
	} else {
		err = w.workload.DoInsert(ctx, w.workerDB)
	}

	if err != nil && !w.p.GetBool(prop.Silence, prop.SilenceDefault) {
		fmt.Printf("operation err: %v\n", err)
	}
	w.wg.Done()
}

// Client is a struct which is used the run workload to a specific DB.
type Client struct {
	p        *properties.Properties
	workload ycsb.Workload
	dbName   string
}

// NewClient returns a client with the given workload and DB.
// The workload and dbName can't be nil.
func NewClient(p *properties.Properties, workload ycsb.Workload, dbname string) *Client {
	return &Client{p: p, workload: workload, dbName: dbname}
}

// Run runs the workload to the target DB, and blocks until all workers end.
func (c *Client) Run(ctx context.Context) {
	var wg sync.WaitGroup
	threadCount := c.p.GetInt(prop.ThreadCount, 1)

	wg.Add(threadCount)
	measureCtx, measureCancel := context.WithCancel(ctx)
	measureCh := make(chan struct{}, 1)
	go func() {
		defer func() {
			measureCh <- struct{}{}
		}()
		// load stage no need to warm up
		if c.p.GetBool(prop.DoTransactions, true) {
			dur := c.p.GetInt64(prop.WarmUpTime, 0)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(dur) * time.Second):
			}
		}
		// finish warming up
		measurement.EnableWarmUp(false)

		dur := c.p.GetInt64(prop.LogInterval, 10)
		t := time.NewTicker(time.Duration(dur) * time.Second)
		defer t.Stop()

		for {
			select {
			case <-t.C:
				measurement.Summary()
			case <-measureCtx.Done():
				return
			}
		}
	}()

	// Create DB for each worker
	dbs := make([]ycsb.DB, threadCount)
	for i := 0; i < threadCount; i++ {
		dbs[i] = CreateDB(c.dbName, c.p)
	}

	for i := 0; i < threadCount; i++ {
		go func(threadId int) {
			defer wg.Done()

			w := newWorker(c.p, threadId, threadCount, c.workload, dbs[threadId])
			ctx := c.workload.InitThread(ctx, threadId, threadCount)
			ctx = w.workerDB.InitThread(ctx, threadId, threadCount)
			w.run(ctx)
			w.workerDB.CleanupThread(ctx)
			c.workload.CleanupThread(ctx)
		}(i)
	}

	wg.Wait()
	//TODO: probably we do not need it
	//if !c.p.GetBool(prop.DoTransactions, true) {
	//	// when loading is finished, try to analyze table if possible.
	//	if analyzeDB, ok := dbs[0].(ycsb.AnalyzeDB); ok {
	//		analyzeDB.Analyze(ctx, c.p.GetString(prop.TableName, prop.TableNameDefault))
	//	}
	//}
	measureCancel()
	<-measureCh
}

func CreateDB(dbName string, p *properties.Properties) ycsb.DB {
	dbCreator := ycsb.GetDBCreator(dbName)
	if dbCreator == nil {
		util.Fatalf("%s is not registered", dbName)
	}
	db, err := dbCreator.Create(p)
	if err != nil {
		util.Fatalf("create db %s failed %v", dbName, err)
	}
	db = DbWrapper{db}
	return db
}
