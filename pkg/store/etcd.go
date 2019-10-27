package store

import (
	"context"
	"fmt"
	v3 "go.etcd.io/etcd/clientv3"
	"time"
)

var (
	defaultTimeout = 5 * time.Second
	prefixLog      = "/storage/ycsb/log/"
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
		panic(err)
	}
	return &etcd{
		client: cli,
	}
}

func (e *etcd) PutOne(key string, val string) error {
	ctx := context.Background()
	t := time.Now().Unix()
	_, err := e.client.Put(ctx, fmt.Sprintf("%s%d", prefixLog, t), val)
	if err != nil {
		fmt.Printf("etcd put error,err=%+v", err)
		return err
	}
	return nil
}

func (e *etcd) Close() error {
	return e.client.Close()
}
