package redis

import (
	"fmt"

	log "github.com/micro/go-micro/v2/logger"
	"github.com/micro/go-micro/v2/store"
	redis "gopkg.in/redis.v5"
)

type rkv struct {
	options store.Options
	Client  *redis.Client
}

func (r *rkv) Init(opts ...store.Option) error {
	for _, o := range opts {
		o(&r.options)
	}

	return r.configure()
}

func (r *rkv) Close() error {
	return r.Client.Close()
}

func (r *rkv) Read(key string, opts ...store.ReadOption) ([]*store.Record, error) {
	options := store.ReadOptions{}
	options.Table = r.options.Table

	for _, o := range opts {
		o(&options)
	}

	records := make([]*store.Record, 0, 1)

	rkey := fmt.Sprintf("%s%s", options.Table, key)
	val, err := r.Client.Get(rkey).Bytes()

	if err != nil && err == redis.Nil {
		return nil, store.ErrNotFound
	} else if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, store.ErrNotFound
	}

	d, err := r.Client.TTL(rkey).Result()
	if err != nil {
		return nil, err
	}

	records = append(records, &store.Record{
		Key:    key,
		Value:  val,
		Expiry: d,
	})

	return records, nil
}

func (r *rkv) Delete(key string, opts ...store.DeleteOption) error {
	options := store.DeleteOptions{}
	options.Table = r.options.Table

	for _, o := range opts {
		o(&options)
	}

	rkey := fmt.Sprintf("%s%s", options.Table, key)
	return r.Client.Del(rkey).Err()
}

func (r *rkv) Write(record *store.Record, opts ...store.WriteOption) error {
	options := store.WriteOptions{}
	options.Table = r.options.Table

	for _, o := range opts {
		o(&options)
	}

	rkey := fmt.Sprintf("%s%s", options.Table, record.Key)
	return r.Client.Set(rkey, record.Value, record.Expiry).Err()
}

func (r *rkv) List(opts ...store.ListOption) ([]string, error) {
	options := store.ListOptions{}
	options.Table = r.options.Table

	for _, o := range opts {
		o(&options)
	}

	keys, err := r.Client.Keys("*").Result()
	if err != nil {
		return nil, err
	}

	return keys, nil
}

func (r *rkv) Options() store.Options {
	return r.options
}

func (r *rkv) String() string {
	return "redis"
}

func NewStore(opts ...store.Option) store.Store {
	var options store.Options
	for _, o := range opts {
		o(&options)
	}

	s := new(rkv)
	s.options = options

	if err := s.configure(); err != nil {
		log.Fatal(err)
	}

	return s
}

func (r *rkv) configure() error {
	nodes := r.options.Nodes

	if len(nodes) == 0 {
		nodes = []string{"127.0.0.1:6379"}
	}

	r.Client = redis.NewClient(&redis.Options{
		Addr:     nodes[0],
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	return nil
}
