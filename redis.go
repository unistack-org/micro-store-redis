package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/unistack-org/micro/v3/store"
)

type rkv struct {
	opts store.Options
	cli  redisClient
}

type redisClient interface {
	Get(ctx context.Context, key string) *redis.StringCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Keys(ctx context.Context, pattern string) *redis.StringSliceCmd
	Exists(ctx context.Context, keys ...string) *redis.IntCmd
	Close() error
}

func (r *rkv) Connect(ctx context.Context) error {
	return nil
}

func (r *rkv) Init(opts ...store.Option) error {
	for _, o := range opts {
		o(&r.opts)
	}

	return r.configure()
}

func (r *rkv) Disconnect(ctx context.Context) error {
	return r.cli.Close()
}

func (r *rkv) Exists(ctx context.Context, key string, opts ...store.ExistsOption) error {
	//options := store.NewReadOptions(opts...)
	//if len(options.Table) == 0 {
	//	options.Table = r.opts.Table
	//}
	rkey := fmt.Sprintf("%s%s", r.opts.Table, key)
	err := r.cli.Exists(ctx, rkey).Err()
	if err != nil {
		return store.ErrNotFound
	}
	return nil
}

func (r *rkv) Read(ctx context.Context, key string, val interface{}, opts ...store.ReadOption) error {
	options := store.NewReadOptions(opts...)
	if len(options.Table) == 0 {
		options.Table = r.opts.Table
	}

	rkey := fmt.Sprintf("%s%s", options.Table, key)
	buf, err := r.cli.Get(ctx, rkey).Bytes()
	if err != nil && err == redis.Nil {
		return store.ErrNotFound
	} else if err != nil {
		return err
	}
	if buf == nil {
		return store.ErrNotFound
	}
	/*
			d, err := r.Client.TTL(rkey).Result()
			if err != nil {
				return nil, err
			}

			records = append(records, &store.Record{
				Key:    key,
				Value:  val,
				Expiry: d,
			})
		}
	*/
	return r.opts.Codec.Unmarshal(buf, val)
}

func (r *rkv) Delete(ctx context.Context, key string, opts ...store.DeleteOption) error {
	options := store.NewDeleteOptions(opts...)
	if len(options.Table) == 0 {
		options.Table = r.opts.Table
	}

	rkey := fmt.Sprintf("%s%s", options.Table, key)
	return r.cli.Del(ctx, rkey).Err()
}

func (r *rkv) Write(ctx context.Context, key string, val interface{}, opts ...store.WriteOption) error {
	options := store.NewWriteOptions(opts...)
	if len(options.Table) == 0 {
		options.Table = r.opts.Table
	}

	rkey := fmt.Sprintf("%s%s", options.Table, key)
	buf, err := r.opts.Codec.Marshal(val)
	if err != nil {
		return err
	}

	return r.cli.Set(ctx, rkey, buf, options.TTL).Err()
}

func (r *rkv) List(ctx context.Context, opts ...store.ListOption) ([]string, error) {
	options := store.NewListOptions(opts...)
	if len(options.Table) == 0 {
		options.Table = r.opts.Table
	}

	// TODO: add support for prefix/suffix/limit
	keys, err := r.cli.Keys(ctx, "*").Result()
	if err != nil {
		return nil, err
	}

	return keys, nil
}

func (r *rkv) Options() store.Options {
	return r.opts
}

func (r *rkv) Name() string {
	return r.opts.Name
}

func (r *rkv) String() string {
	return "redis"
}

func NewStore(opts ...store.Option) store.Store {
	return &rkv{opts: store.NewOptions(opts...)}
}

func (r *rkv) configure() error {
	var redisOptions *redis.Options
	var redisClusterOptions *redis.ClusterOptions
	var err error

	nodes := r.opts.Nodes

	if len(nodes) == 0 {
		nodes = []string{"redis://127.0.0.1:6379"}
	}

	if r.opts.Context != nil {
		if c, ok := r.opts.Context.Value(configKey{}).(*redis.Options); ok {
			redisOptions = c
		}

		if c, ok := r.opts.Context.Value(clusterConfigKey{}).(*redis.ClusterOptions); ok {
			redisClusterOptions = c
		}
	}

	if redisOptions != nil && redisClusterOptions != nil {
		return fmt.Errorf("must specify only one option Config or ClusterConfig")
	}

	if redisOptions == nil && redisClusterOptions == nil && len(nodes) == 1 {
		redisOptions, err = redis.ParseURL(nodes[0])
		if err != nil {
			//Backwards compatibility
			redisOptions = &redis.Options{
				Addr:     nodes[0],
				Password: "", // no password set
				DB:       0,  // use default DB
			}
		}
	} else if redisOptions == nil && redisClusterOptions == nil && len(nodes) > 1 {
		redisClusterOptions = &redis.ClusterOptions{Addrs: nodes}
	}

	if redisOptions != nil {
		r.cli = redis.NewClient(redisOptions)
	} else if redisClusterOptions != nil {
		r.cli = redis.NewClusterClient(redisClusterOptions)
	}

	return nil
}
