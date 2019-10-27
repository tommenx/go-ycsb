package store

var LogDB DB

type DB interface {
	PutOne(key string, val string) error
	Close() error
}
