package store

import (
	goredis "github.com/go-redis/redis"
	"time"
)

type redis struct {
	client *goredis.Client
}

func NewRedis(addr string) DB {
	cli := goredis.NewClient(&goredis.Options{
		Addr:     addr,
		Password: "",
		DB:       0,
	})
	return &redis{
		client: cli,
	}
}

func (r *redis) PutOne(key string, val string) error {
	cmd := r.client.ZAdd(key, goredis.Z{
		Score:  float64(time.Now().UnixNano()),
		Member: val,
	})
	return cmd.Err()
}

func (r *redis) Close() error {
	return r.client.Close()
}
