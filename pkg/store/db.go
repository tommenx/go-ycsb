package store

var LogDB DB

type DB interface {
	PutOne(job string, val string, kind int) error
	Watch(key string, handlerFunc func(val string))
	GetOne(key string) int
	Close() error
}
