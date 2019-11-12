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

package main

import (
	"context"
	"github.com/pingcap/go-ycsb/pkg/label"
	"github.com/pingcap/go-ycsb/pkg/store"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/magiconair/properties"

	// Register workload

	"fmt"

	"github.com/pingcap/go-ycsb/pkg/client"
	"github.com/pingcap/go-ycsb/pkg/measurement"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"
	_ "github.com/pingcap/go-ycsb/pkg/workload"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"github.com/spf13/cobra"

	// Register basic database
	_ "github.com/pingcap/go-ycsb/db/basic"
	// Register MySQL database
	_ "github.com/pingcap/go-ycsb/db/mysql"
	// Register TiKV database
	_ "github.com/pingcap/go-ycsb/db/tikv"
	// Register PostgreSQL database
	_ "github.com/pingcap/go-ycsb/db/pg"
	// Register Aerospike database
	_ "github.com/pingcap/go-ycsb/db/aerospike"
	// Register Badger database
	_ "github.com/pingcap/go-ycsb/db/badger"
	// Register RocksDB database
	_ "github.com/pingcap/go-ycsb/db/rocksdb"
	// Register Spanner database
	_ "github.com/pingcap/go-ycsb/db/spanner"
	// Register pegasus database
	_ "github.com/pingcap/go-ycsb/db/pegasus"
	// Register sqlite database
	_ "github.com/pingcap/go-ycsb/db/sqlite"
	// Register cassandra database
	_ "github.com/pingcap/go-ycsb/db/cassandra"
	// Register mongodb database
	_ "github.com/pingcap/go-ycsb/db/mongodb"
	// Register redis database
	_ "github.com/pingcap/go-ycsb/db/redis"
	// Register boltdb database
	_ "github.com/pingcap/go-ycsb/db/boltdb"
)

var (
	propertyFiles  []string
	propertyValues []string
	dbName         string
	tableName      string

	globalContext context.Context
	globalCancel  context.CancelFunc

	globalDB       ycsb.DB
	globalWorkload ycsb.Workload
	globalProps    *properties.Properties
)

func initialGlobal(dbName string, onProperties func()) {
	globalProps = properties.NewProperties()
	if len(propertyFiles) > 0 {
		globalProps = properties.MustLoadFiles(propertyFiles, properties.UTF8, false)
	}

	for _, prop := range propertyValues {
		seps := strings.SplitN(prop, "=", 2)
		globalProps.Set(seps[0], seps[1])
	}

	if onProperties != nil {
		onProperties()
	}

	addr := globalProps.GetString(prop.DebugPprof, prop.DebugPprofDefault)
	go func() {
		http.ListenAndServe(addr, nil)
	}()

	measurement.InitMeasure(globalProps)

	if len(tableName) == 0 {
		tableName = globalProps.GetString(prop.TableName, prop.TableNameDefault)
	}

	workloadName := globalProps.GetString(prop.Workload, "core")
	workloadCreator := ycsb.GetWorkloadCreator(workloadName)

	var err error
	if globalWorkload, err = workloadCreator.Create(globalProps); err != nil {
		util.Fatalf("create workload %s failed %v", workloadName, err)
	}

	dbCreator := ycsb.GetDBCreator(dbName)
	if dbCreator == nil {
		util.Fatalf("%s is not registered", dbName)
	}
	if globalDB, err = dbCreator.Create(globalProps); err != nil {
		util.Fatalf("create db %s failed %v", dbName, err)
	}
	globalDB = client.DbWrapper{globalDB}
	if label.Log != "none" {
		store.LogDB = store.NewEtcd(label.StoreAddr)
		store.LogDB.PutOne(label.JobName, fmt.Sprintf("%d", label.OperationCount), label.PREFIX_REQUEST_OPERATION)
		label.QPS = store.LogDB.GetOne("/storage/setting/qps")
		go store.LogDB.Watch("/storage/setting/qps", func(val string) {
			settingQPS, _ := strconv.Atoi(val)
			label.QPS = settingQPS
		})
	}
}

func main() {
	globalContext, globalCancel = context.WithCancel(context.Background())

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	closeDone := make(chan struct{}, 1)
	go func() {
		sig := <-sc
		fmt.Printf("\nGot signal [%v] to exit.\n", sig)
		globalCancel()

		select {
		case <-sc:
			// send signal again, return directly
			fmt.Printf("\nGot signal [%v] again to exit.\n", sig)
			os.Exit(1)
		case <-time.After(10 * time.Second):
			fmt.Print("\nWait 10s for closed, force exit\n")
			os.Exit(1)
		case <-closeDone:
			return
		}
	}()
	//updateQPS()
	//updateTicker := time.NewTicker(time.Second * 5)
	//go func() {
	//	for {
	//		select {
	//		case <-updateTicker.C:
	//			go updateQPS()
	//		}
	//	}
	//}()

	rootCmd := &cobra.Command{
		Use:   "go-ycsb",
		Short: "Go YCSB",
	}

	rootCmd.AddCommand(
		newShellCommand(),
		newLoadCommand(),
		newRunCommand(),
	)

	cobra.EnablePrefixMatching = true

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(rootCmd.UsageString())
	}

	globalCancel()
	if globalDB != nil {
		globalDB.Close()
	}

	if globalWorkload != nil {
		globalWorkload.Close()
	}

	closeDone <- struct{}{}
}

func updateQPS() {
	qps, err := ioutil.ReadFile("qps")
	qpsStr := strings.Trim(string(qps), " \n")
	qpsInt, err := strconv.Atoi(qpsStr)
	if err != nil {
		fmt.Printf("set qps from file error, err=%+v", err)
		qpsInt = 0
	}
	label.QPS = qpsInt
	if label.Log != "none" && store.LogDB != nil {
		if err := store.LogDB.PutOne(label.JobName, fmt.Sprintf("%d", qpsInt), label.PREFIX_REQUEST_QPS); err != nil {
			fmt.Printf("set request qps error, err=%+v", err)
		}
	}
}
