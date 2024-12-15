package redis

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"go.unistack.org/micro/v4/options"
	"go.unistack.org/micro/v4/store"
)

type Store struct {
	opts store.Options
	cli  redis.UniversalClient
}

func (r *Store) Connect(ctx context.Context) error {
	return r.cli.Ping(ctx).Err()
}

func (r *Store) Init(opts ...options.Option) error {
	return r.configure(opts...)
}

func (r *Store) Redis() redis.UniversalClient {
	return r.cli
}

func (r *Store) Disconnect(ctx context.Context) error {
	return r.cli.Close()
}

func (r *Store) Exists(ctx context.Context, key string, opts ...options.Option) error {
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

func (r *Store) Read(ctx context.Context, key string, val interface{}, opts ...options.Option) error {
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

func (r *Store) MRead(ctx context.Context, keys []string, vals interface{}, opts ...options.Option) error {
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

func (r *Store) MDelete(ctx context.Context, keys []string, opts ...options.Option) error {
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

func (r *Store) Delete(ctx context.Context, key string, opts ...options.Option) error {
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

func (r *Store) MWrite(ctx context.Context, keys []string, vals []interface{}, opts ...options.Option) error {
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
		var err error
		for idx := 0; idx < len(kvs); idx += 2 {
			if _, err = pipe.Set(ctx, kvs[idx], kvs[idx+1], options.TTL).Result(); err != nil {
				return err
			}
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

func (r *Store) Write(ctx context.Context, key string, val interface{}, opts ...options.Option) error {
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

func (r *Store) List(ctx context.Context, opts ...options.Option) ([]string, error) {
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

func NewStore(opts ...options.Option) *Store {
	return &Store{opts: store.NewOptions(opts...)}
}

func (r *Store) configure(opts ...options.Option) error {
	if r.cli != nil && len(opts) == 0 {
		return nil
	}

	var redisUniversalOptions *redis.UniversalOptions
	var err error

	for _, o := range opts {
		if err = o(r.opts); err != nil {
			return err
		}
	}

	if r.opts.Context != nil {
		if c, ok := r.opts.Context.Value(universalConfigKey{}).(*redis.UniversalOptions); ok {
			redisUniversalOptions = c
			if r.opts.TLSConfig != nil {
				redisUniversalOptions.TLSConfig = r.opts.TLSConfig
			}
		}
	}

	if redisUniversalOptions == nil && r.cli != nil {
		return nil
	}

	if redisUniversalOptions == nil {
		redisUniversalOptions = &redis.UniversalOptions{
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

	if len(r.opts.Address) > 0 {
		redisUniversalOptions.Addrs = r.opts.Address
	} else if len(redisUniversalOptions.Addrs) == 0 {
		redisUniversalOptions.Addrs = []string{"redis://127.0.0.1:6379"}
	}

	r.cli = redis.NewUniversalClient(redisUniversalOptions)

	return nil
}
