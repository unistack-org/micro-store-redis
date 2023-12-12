package redis

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"go.unistack.org/micro/v3/store"
)

type Store struct {
	opts store.Options
	cli  redisClient
}

type redisClient interface {
	Get(ctx context.Context, key string) *redis.StringCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Keys(ctx context.Context, pattern string) *redis.StringSliceCmd
	MGet(ctx context.Context, keys ...string) *redis.SliceCmd
	MSet(ctx context.Context, kv ...interface{}) *redis.StatusCmd
	Exists(ctx context.Context, keys ...string) *redis.IntCmd
	Ping(ctx context.Context) *redis.StatusCmd
	Pipeline() redis.Pipeliner
	Pipelined(ctx context.Context, fn func(redis.Pipeliner) error) ([]redis.Cmder, error)
	Close() error
}

func (r *Store) Connect(ctx context.Context) error {
	return r.cli.Ping(ctx).Err()
}

func (r *Store) Init(opts ...store.Option) error {
	for _, o := range opts {
		o(&r.opts)
	}

	return r.configure()
}

func (r *Store) Redis() *redis.Client {
	return r.cli.(*redis.Client)
}

func (r *Store) Disconnect(ctx context.Context) error {
	return r.cli.Close()
}

func (r *Store) Exists(ctx context.Context, key string, opts ...store.ExistsOption) error {
	if r.opts.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.opts.Timeout)
		defer cancel()
	}
	options := store.NewExistsOptions(opts...)
	if len(options.Namespace) == 0 {
		options.Namespace = r.opts.Namespace
	}
	if options.Namespace != "" {
		key = fmt.Sprintf("%s%s", r.opts.Namespace, key)
	}
	val, err := r.cli.Exists(ctx, key).Result()
	if err != nil {
		return err
	}
	if val == 0 {
		return store.ErrNotFound
	}
	return nil
}

func (r *Store) Read(ctx context.Context, key string, val interface{}, opts ...store.ReadOption) error {
	if r.opts.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.opts.Timeout)
		defer cancel()
	}
	options := store.NewReadOptions(opts...)
	if len(options.Namespace) == 0 {
		options.Namespace = r.opts.Namespace
	}
	if options.Namespace != "" {
		key = fmt.Sprintf("%s%s", options.Namespace, key)
	}

	buf, err := r.cli.Get(ctx, key).Bytes()
	if err != nil && err == redis.Nil {
		return store.ErrNotFound
	} else if err != nil {
		return err
	}
	if buf == nil {
		return store.ErrNotFound
	}
	return r.opts.Codec.Unmarshal(buf, val)
}

func (r *Store) MRead(ctx context.Context, keys []string, vals interface{}, opts ...store.ReadOption) error {
	if len(keys) == 1 {
		vt := reflect.ValueOf(vals)
		if vt.Kind() == reflect.Ptr {
			vt = reflect.Indirect(vt)
			return r.Read(ctx, keys[0], vt.Index(0).Interface(), opts...)
		}
	}
	if r.opts.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.opts.Timeout)
		defer cancel()
	}
	options := store.NewReadOptions(opts...)
	rkeys := make([]string, 0, len(keys))
	if len(options.Namespace) == 0 {
		options.Namespace = r.opts.Namespace
	}
	for _, key := range keys {
		if options.Namespace != "" {
			rkeys = append(rkeys, fmt.Sprintf("%s%s", options.Namespace, key))
		} else {
			rkeys = append(rkeys, key)
		}
	}

	rvals, err := r.cli.MGet(ctx, rkeys...).Result()
	if err != nil && err == redis.Nil {
		return store.ErrNotFound
	} else if err != nil {
		return err
	}
	if len(rvals) == 0 {
		return store.ErrNotFound
	}

	vv := reflect.ValueOf(vals)
	vt := reflect.TypeOf(vals)
	switch vv.Kind() {
	case reflect.Ptr:
		vv = reflect.Indirect(vv)
		vt = vt.Elem()
	}
	if vv.Kind() != reflect.Slice {
		return store.ErrNotFound
	}
	nvv := reflect.MakeSlice(vt, len(rvals), len(rvals))
	vt = vt.Elem()
	for idx := 0; idx < len(rvals); idx++ {
		if rvals[idx] == nil {
			continue
		}

		itm := nvv.Index(idx)
		var buf []byte
		switch b := rvals[idx].(type) {
		case []byte:
			buf = b
		case string:
			buf = []byte(b)
		}
		// special case for raw data
		if vt.Kind() == reflect.Slice && vt.Elem().Kind() == reflect.Uint8 {
			itm.Set(reflect.MakeSlice(itm.Type(), len(buf), len(buf)))
		} else {
			itm.Set(reflect.New(vt.Elem()))
		}
		if err = r.opts.Codec.Unmarshal(buf, itm.Interface()); err != nil {
			return err
		}
	}
	vv.Set(nvv)
	return nil
}

func (r *Store) MDelete(ctx context.Context, keys []string, opts ...store.DeleteOption) error {
	if len(keys) == 1 {
		return r.Delete(ctx, keys[0], opts...)
	}
	if r.opts.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.opts.Timeout)
		defer cancel()
	}
	options := store.NewDeleteOptions(opts...)
	if len(options.Namespace) == 0 {
		options.Namespace = r.opts.Namespace
	}
	if options.Namespace == "" {
		return r.cli.Del(ctx, keys...).Err()
	}
	for idx := range keys {
		keys[idx] = options.Namespace + keys[idx]
	}
	return r.cli.Del(ctx, keys...).Err()
}

func (r *Store) Delete(ctx context.Context, key string, opts ...store.DeleteOption) error {
	if r.opts.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.opts.Timeout)
		defer cancel()
	}
	options := store.NewDeleteOptions(opts...)
	if len(options.Namespace) == 0 {
		options.Namespace = r.opts.Namespace
	}
	if options.Namespace == "" {
		return r.cli.Del(ctx, key).Err()
	}
	return r.cli.Del(ctx, fmt.Sprintf("%s%s", options.Namespace, key)).Err()
}

func (r *Store) MWrite(ctx context.Context, keys []string, vals []interface{}, opts ...store.WriteOption) error {
	if len(keys) == 1 {
		return r.Write(ctx, keys[0], vals[0], opts...)
	}
	if r.opts.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.opts.Timeout)
		defer cancel()
	}
	options := store.NewWriteOptions(opts...)
	kvs := make([]string, 0, len(keys)*2)

	if len(options.Namespace) == 0 {
		options.Namespace = r.opts.Namespace
	}

	for idx, key := range keys {
		if options.Namespace != "" {
			kvs = append(kvs, fmt.Sprintf("%s%s", options.Namespace, key))
		} else {
			kvs = append(kvs, key)
		}

		switch vt := vals[idx].(type) {
		case string:
			kvs = append(kvs, vt)
		case []byte:
			kvs = append(kvs, string(vt))
		default:
			buf, err := r.opts.Codec.Marshal(vt)
			if err != nil {
				return err
			}
			kvs = append(kvs, string(buf))
		}
	}

	cmds, err := r.cli.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for idx := 0; idx < len(kvs); idx += 2 {
			pipe.Set(ctx, kvs[idx], kvs[idx+1], options.TTL).Result()
		}
		return nil
	})
	if err != nil {
		return err
	}

	for _, cmd := range cmds {
		if err = cmd.Err(); err != nil {
			return err
		}
	}

	return nil
}

func (r *Store) Write(ctx context.Context, key string, val interface{}, opts ...store.WriteOption) error {
	if r.opts.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.opts.Timeout)
		defer cancel()
	}

	options := store.NewWriteOptions(opts...)
	if len(options.Namespace) == 0 {
		options.Namespace = r.opts.Namespace
	}

	var buf []byte
	switch vt := val.(type) {
	case string:
		buf = []byte(vt)
	case []byte:
		buf = vt
	default:
		var err error
		buf, err = r.opts.Codec.Marshal(val)
		if err != nil {
			return err
		}
	}

	if options.Namespace != "" {
		key = fmt.Sprintf("%s%s", options.Namespace, key)
	}

	return r.cli.Set(ctx, key, buf, options.TTL).Err()
}

func (r *Store) List(ctx context.Context, opts ...store.ListOption) ([]string, error) {
	options := store.NewListOptions(opts...)
	if len(options.Namespace) == 0 {
		options.Namespace = r.opts.Namespace
	}

	rkey := fmt.Sprintf("%s%s*", options.Namespace, options.Prefix)
	if options.Suffix != "" {
		rkey += options.Suffix
	}
	if r.opts.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.opts.Timeout)
		defer cancel()
	}
	// TODO: add support for prefix/suffix/limit
	keys, err := r.cli.Keys(ctx, rkey).Result()
	if err != nil {
		return nil, err
	}
	if options.Namespace == "" {
		return keys, nil
	}

	nkeys := make([]string, 0, len(keys))
	for _, key := range keys {
		nkeys = append(nkeys, strings.TrimPrefix(key, options.Namespace))
	}
	return nkeys, nil
}

func (r *Store) Options() store.Options {
	return r.opts
}

func (r *Store) Name() string {
	return r.opts.Name
}

func (r *Store) String() string {
	return "redis"
}

func NewStore(opts ...store.Option) *Store {
	return &Store{opts: store.NewOptions(opts...)}
}

func (r *Store) configure() error {
	var redisOptions *redis.Options
	var redisClusterOptions *redis.ClusterOptions
	var err error

	nodes := r.opts.Addrs

	if len(nodes) == 0 {
		nodes = []string{"redis://127.0.0.1:6379"}
	}

	if r.cli != nil && r.opts.Context == nil {
		return nil
	}

	if r.opts.Context != nil {
		if c, ok := r.opts.Context.Value(configKey{}).(*redis.Options); ok {
			redisOptions = c
			if r.opts.TLSConfig != nil {
				redisOptions.TLSConfig = r.opts.TLSConfig
			}
		}

		if c, ok := r.opts.Context.Value(clusterConfigKey{}).(*redis.ClusterOptions); ok {
			redisClusterOptions = c
			if r.opts.TLSConfig != nil {
				redisClusterOptions.TLSConfig = r.opts.TLSConfig
			}
		}
	}

	if redisOptions != nil && redisClusterOptions != nil {
		return fmt.Errorf("must specify only one option Config or ClusterConfig")
	}

	if redisOptions == nil && redisClusterOptions == nil && r.cli != nil {
		return nil
	}

	if redisOptions == nil && redisClusterOptions == nil && len(nodes) == 1 {
		redisOptions, err = redis.ParseURL(nodes[0])
		if err != nil {
			// Backwards compatibility
			redisOptions = &redis.Options{
				Addr:            nodes[0],
				Username:        "",
				Password:        "", // no password set
				DB:              0,  // use default DB
				MaxRetries:      2,
				MaxRetryBackoff: 256 * time.Millisecond,
				DialTimeout:     1 * time.Second,
				ReadTimeout:     1 * time.Second,
				WriteTimeout:    1 * time.Second,
				PoolTimeout:     1 * time.Second,
				MinIdleConns:    10,
				TLSConfig:       r.opts.TLSConfig,
			}
		}
	} else if redisOptions == nil && redisClusterOptions == nil && len(nodes) > 1 {
		redisClusterOptions = &redis.ClusterOptions{
			Addrs:           nodes,
			Username:        "",
			Password:        "", // no password set
			MaxRetries:      2,
			MaxRetryBackoff: 256 * time.Millisecond,
			DialTimeout:     1 * time.Second,
			ReadTimeout:     1 * time.Second,
			WriteTimeout:    1 * time.Second,
			PoolTimeout:     1 * time.Second,
			MinIdleConns:    10,
			TLSConfig:       r.opts.TLSConfig,
		}
	}

	if redisOptions != nil {
		r.cli = redis.NewClient(redisOptions)
	} else if redisClusterOptions != nil {
		r.cli = redis.NewClusterClient(redisClusterOptions)
	}

	return nil
}
