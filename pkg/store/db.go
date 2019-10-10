package store

import (
	goredis "github.com/go-redis/redis"
	"time"
)

var LogDB redisClient

type redisClient interface {
	ZAdd(key string, val string) error
	ZRange(keys []string) (map[string][]string, error)
	LPush(key, val string) error
	LRange(key string) ([]string, error)
	Close() error
}

type redis struct {
	client *goredis.Client
}

func NewRedis(addr string) redisClient {
	cli := goredis.NewClient(&goredis.Options{
		Addr:     addr,
		Password: "",
		DB:       0,
	})
	return &redis{
		client: cli,
	}
}

func (r *redis) ZAdd(key string, val string) error {
	cmd := r.client.ZAdd(key, goredis.Z{
		Score:  float64(time.Now().UnixNano()),
		Member: val,
	})
	return cmd.Err()
}

func (r *redis) ZRange(keys []string) (map[string][]string, error) {
	res := make(map[string][]string)
	for _, key := range keys {
		cmd := r.client.ZRange(key, 0, -1)
		if cmd.Err() != nil {
			return nil, cmd.Err()
		}
		res[key] = cmd.Val()
	}
	return res, nil
}

func (r *redis) LPush(key, val string) error {
	cmd := r.client.LPush(key, val)
	return cmd.Err()
}

func (r *redis) LRange(key string) ([]string, error) {
	cmd := r.client.LRange(key, 0, -1)
	return cmd.Result()
}

func (r *redis) Close() error {
	return r.client.Close()
}
