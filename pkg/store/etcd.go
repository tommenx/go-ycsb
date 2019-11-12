package store

import (
	"context"
	"fmt"
	"github.com/pingcap/go-ycsb/pkg/label"
	v3 "go.etcd.io/etcd/clientv3"
	"os"
	"strconv"
	"time"
)

var (
	defaultTimeout         = 5 * time.Second
	prefixLogQPS           = "/storage/ycsb/log/"
	prefixRequestQPS       = "/storage/ycsb/requestQPS/"
	prefixLogOperation     = "/storage/ycsb/operation/"
	prefixRequestOperation = "/storage/ycsb/reuqestOperation/"
	prefixWatchQPS         = "/storage/setting/qps"
)

type etcd struct {
	client *v3.Client
}

func NewEtcd(endpoint string) DB {
	endpoints := []string{endpoint}
	cli, err := v3.New(v3.Config{
		Endpoints:   endpoints,
		DialTimeout: defaultTimeout,
	})
	if err != nil {
		fmt.Printf("new client error, err=%+v", err)
		os.Exit(2)
	}
	return &etcd{
		client: cli,
	}
}

func (e *etcd) PutOne(job string, val string, kind int) error {
	ctx := context.Background()
	var prefix string
	if kind == label.PREFIX_LOG_QPS {
		prefix = prefixLogQPS
	} else if kind == label.PREFIX_REQUEST_QPS {
		prefix = prefixRequestQPS
	} else if kind == label.PREFIX_LOG_OPERATION {
		prefix = prefixLogOperation
	} else if kind == label.PREFIX_REQUEST_OPERATION {
		prefix = prefixRequestOperation
	}
	_, err := e.client.Put(ctx, fmt.Sprintf("%s%s", prefix, job), val)
	if err != nil {
		fmt.Printf("etcd put error,err=%+v", err)
		return err
	}
	return nil
}

func (e *etcd) watch(key string) v3.WatchChan {
	return e.client.Watch(context.Background(), key)
}

func (e *etcd) Watch(key string, handlerFunc func(val string)) {
	ch := e.watch(key)
	for {
		select {
		case event := <-ch:
			for _, v := range event.Events {
				if v.Type == v3.EventTypePut {
					handlerFunc(string(v.Kv.Value))
				}
			}
		}
	}
}

func (e *etcd) GetOne(key string) int {
	data, err := e.client.Get(context.Background(), "/storage/setting/qps")
	if err != nil {
		fmt.Printf("get %s error,err=%+v", key, err)
		return 200
	}
	if len(data.Kvs) == 0 {
		return 200
	}
	if qps, err := strconv.Atoi(string(data.Kvs[0].Value)); err != nil {
		fmt.Printf("parse qps error, err=%+v", err)
		return 200
	} else {
		return qps
	}
}

func (e *etcd) Close() error {
	return e.client.Close()
}
