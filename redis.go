package redis

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	redis "github.com/redis/go-redis/v9"
	"go.unistack.org/micro/v3/semconv"
	"go.unistack.org/micro/v3/store"
	"go.unistack.org/micro/v3/tracer"
	pool "go.unistack.org/micro/v3/util/xpool"
)

var (
	DefaultPathSeparator = "/"

	DefaultClusterOptions = &redis.ClusterOptions{
		Username:        "",
		Password:        "", // no password set
		MaxRetries:      2,
		MaxRetryBackoff: 256 * time.Millisecond,
		DialTimeout:     1 * time.Second,
		ReadTimeout:     1 * time.Second,
		WriteTimeout:    1 * time.Second,
		PoolTimeout:     1 * time.Second,
		MinIdleConns:    10,
	}

	DefaultOptions = &redis.Options{
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
	}
)

type Store struct {
	opts store.Options
	cli  redisClient
	pool pool.Pool[strings.Builder]
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
	options := store.NewExistsOptions(opts...)

	timeout := r.opts.Timeout
	if options.Timeout > 0 {
		timeout = options.Timeout
	}

	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	rkey := r.getKey(r.opts.Namespace, options.Namespace, key)
	ctx, sp := r.opts.Tracer.Start(ctx, "cache exists "+rkey)
	defer sp.Finish()

	r.opts.Meter.Counter(semconv.CacheRequestInflight, "name", options.Name).Inc()
	ts := time.Now()
	val, err := r.cli.Exists(ctx, rkey).Result()
	te := time.Since(ts)
	r.opts.Meter.Counter(semconv.CacheRequestInflight, "name", options.Name).Dec()
	r.opts.Meter.Summary(semconv.CacheRequestLatencyMicroseconds, "name", options.Name).Update(te.Seconds())
	r.opts.Meter.Histogram(semconv.CacheRequestDurationSeconds, "name", options.Name).Update(te.Seconds())
	if err == redis.Nil || (err == nil && val == 0) {
		r.opts.Meter.Counter(semconv.CacheRequestTotal, "name", options.Name, "status", "miss").Inc()
		return store.ErrNotFound
	} else if err == nil {
		r.opts.Meter.Counter(semconv.CacheRequestTotal, "name", options.Name, "status", "hit").Inc()
	} else if err != nil {
		sp.SetStatus(tracer.SpanStatusError, err.Error())
		r.opts.Meter.Counter(semconv.CacheRequestTotal, "name", options.Name, "status", "failure").Inc()
		return err
	}

	return nil
}

func (r *Store) Read(ctx context.Context, key string, val interface{}, opts ...store.ReadOption) error {
	options := store.NewReadOptions(opts...)

	timeout := r.opts.Timeout
	if options.Timeout > 0 {
		timeout = options.Timeout
	}

	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	rkey := r.getKey(r.opts.Namespace, options.Namespace, key)
	ctx, sp := r.opts.Tracer.Start(ctx, "cache read "+rkey)
	defer sp.Finish()

	r.opts.Meter.Counter(semconv.CacheRequestInflight, "name", options.Name).Inc()
	ts := time.Now()
	buf, err := r.cli.Get(ctx, rkey).Bytes()
	te := time.Since(ts)
	r.opts.Meter.Counter(semconv.CacheRequestInflight, "name", options.Name).Dec()
	r.opts.Meter.Summary(semconv.CacheRequestLatencyMicroseconds, "name", options.Name).Update(te.Seconds())
	r.opts.Meter.Histogram(semconv.CacheRequestDurationSeconds, "name", options.Name).Update(te.Seconds())
	if err == redis.Nil || (err == nil && buf == nil) {
		r.opts.Meter.Counter(semconv.CacheRequestTotal, "name", options.Name, "status", "miss").Inc()
		return store.ErrNotFound
	} else if err == nil {
		r.opts.Meter.Counter(semconv.CacheRequestTotal, "name", options.Name, "status", "hit").Inc()
	} else if err != nil {
		sp.SetStatus(tracer.SpanStatusError, err.Error())
		r.opts.Meter.Counter(semconv.CacheRequestTotal, "name", options.Name, "status", "failure").Inc()
		return err
	}

	switch b := val.(type) {
	case *[]byte:
		*b = buf
	case *string:
		*b = string(buf)
	default:
		if err = r.opts.Codec.Unmarshal(buf, val); err != nil {
			sp.SetStatus(tracer.SpanStatusError, err.Error())
		}
	}

	return err
}

func (r *Store) MRead(ctx context.Context, keys []string, vals interface{}, opts ...store.ReadOption) error {
	options := store.NewReadOptions(opts...)

	timeout := r.opts.Timeout
	if options.Timeout > 0 {
		timeout = options.Timeout
	}

	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	if r.opts.Namespace != "" || options.Namespace != "" {
		for idx, key := range keys {
			keys[idx] = r.getKey(r.opts.Namespace, options.Namespace, key)
		}
	}

	ctx, sp := r.opts.Tracer.Start(ctx, fmt.Sprintf("cache mread %v", keys))
	defer sp.Finish()

	r.opts.Meter.Counter(semconv.CacheRequestInflight, "name", options.Name).Inc()
	ts := time.Now()
	rvals, err := r.cli.MGet(ctx, keys...).Result()
	te := time.Since(ts)
	r.opts.Meter.Counter(semconv.CacheRequestInflight, "name", options.Name).Dec()
	r.opts.Meter.Summary(semconv.CacheRequestLatencyMicroseconds, "name", options.Name).Update(te.Seconds())
	r.opts.Meter.Histogram(semconv.CacheRequestDurationSeconds, "name", options.Name).Update(te.Seconds())
	if err == redis.Nil || (len(rvals) == 0) {
		r.opts.Meter.Counter(semconv.CacheRequestTotal, "name", options.Name, "status", "miss").Inc()
		return store.ErrNotFound
	} else if err == nil {
		r.opts.Meter.Counter(semconv.CacheRequestTotal, "name", options.Name, "status", "hit").Inc()
	} else if err != nil {
		sp.SetStatus(tracer.SpanStatusError, err.Error())
		r.opts.Meter.Counter(semconv.CacheRequestTotal, "name", options.Name, "status", "failure").Inc()
		return err
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
			itm.SetBytes(buf)
			continue
		} else if vt.Kind() == reflect.String {
			itm.SetString(string(buf))
			continue
		}

		itm.Set(reflect.New(vt.Elem()))
		if err = r.opts.Codec.Unmarshal(buf, itm.Interface()); err != nil {
			sp.SetStatus(tracer.SpanStatusError, err.Error())
			return err
		}
	}
	vv.Set(nvv)

	return nil
}

func (r *Store) MDelete(ctx context.Context, keys []string, opts ...store.DeleteOption) error {
	options := store.NewDeleteOptions(opts...)

	timeout := r.opts.Timeout
	if options.Timeout > 0 {
		timeout = options.Timeout
	}

	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	if r.opts.Namespace != "" || options.Namespace != "" {
		for idx, key := range keys {
			keys[idx] = r.getKey(r.opts.Namespace, options.Namespace, key)
		}
	}

	ctx, sp := r.opts.Tracer.Start(ctx, fmt.Sprintf("cache mdelete %v", keys))
	defer sp.Finish()

	r.opts.Meter.Counter(semconv.CacheRequestInflight, "name", options.Name).Inc()
	ts := time.Now()
	err := r.cli.Del(ctx, keys...).Err()
	te := time.Since(ts)
	r.opts.Meter.Counter(semconv.CacheRequestInflight, "name", options.Name).Dec()
	r.opts.Meter.Summary(semconv.CacheRequestLatencyMicroseconds, "name", options.Name).Update(te.Seconds())
	r.opts.Meter.Histogram(semconv.CacheRequestDurationSeconds, "name", options.Name).Update(te.Seconds())
	if err == redis.Nil {
		r.opts.Meter.Counter(semconv.CacheRequestTotal, "name", options.Name, "status", "miss").Inc()
		return store.ErrNotFound
	} else if err == nil {
		r.opts.Meter.Counter(semconv.CacheRequestTotal, "name", options.Name, "status", "hit").Inc()
	} else if err != nil {
		sp.SetStatus(tracer.SpanStatusError, err.Error())
		r.opts.Meter.Counter(semconv.CacheRequestTotal, "name", options.Name, "status", "failure").Inc()
		return err
	}

	return nil
}

func (r *Store) Delete(ctx context.Context, key string, opts ...store.DeleteOption) error {
	options := store.NewDeleteOptions(opts...)

	timeout := r.opts.Timeout
	if options.Timeout > 0 {
		timeout = options.Timeout
	}

	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	ctx, sp := r.opts.Tracer.Start(ctx, fmt.Sprintf("cache delete %v", key))
	defer sp.Finish()

	r.opts.Meter.Counter(semconv.CacheRequestInflight, "name", options.Name).Inc()
	ts := time.Now()
	err := r.cli.Del(ctx, r.getKey(r.opts.Namespace, options.Namespace, key)).Err()
	te := time.Since(ts)
	r.opts.Meter.Counter(semconv.CacheRequestInflight, "name", options.Name).Dec()
	r.opts.Meter.Summary(semconv.CacheRequestLatencyMicroseconds, "name", options.Name).Update(te.Seconds())
	r.opts.Meter.Histogram(semconv.CacheRequestDurationSeconds, "name", options.Name).Update(te.Seconds())
	if err == redis.Nil {
		r.opts.Meter.Counter(semconv.CacheRequestTotal, "name", options.Name, "status", "miss").Inc()
		return store.ErrNotFound
	} else if err == nil {
		r.opts.Meter.Counter(semconv.CacheRequestTotal, "name", options.Name, "status", "hit").Inc()
	} else if err != nil {
		sp.SetStatus(tracer.SpanStatusError, err.Error())
		r.opts.Meter.Counter(semconv.CacheRequestTotal, "name", options.Name, "status", "failure").Inc()
		return err
	}

	return nil
}

func (r *Store) MWrite(ctx context.Context, keys []string, vals []interface{}, opts ...store.WriteOption) error {
	options := store.NewWriteOptions(opts...)

	timeout := r.opts.Timeout
	if options.Timeout > 0 {
		timeout = options.Timeout
	}

	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	ctx, sp := r.opts.Tracer.Start(ctx, fmt.Sprintf("cache mwrite %v", keys))
	defer sp.Finish()

	kvs := make([]string, 0, len(keys)*2)

	for idx, key := range keys {
		kvs = append(kvs, r.getKey(r.opts.Namespace, options.Namespace, key))

		switch vt := vals[idx].(type) {
		case string:
			kvs = append(kvs, vt)
		case []byte:
			kvs = append(kvs, string(vt))
		default:
			buf, err := r.opts.Codec.Marshal(vt)
			if err != nil {
				sp.SetStatus(tracer.SpanStatusError, err.Error())
				return err
			}
			kvs = append(kvs, string(buf))
		}
	}

	r.opts.Meter.Counter(semconv.CacheRequestInflight, "name", options.Name).Inc()
	ts := time.Now()

	cmds, err := r.cli.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for idx := 0; idx < len(kvs); idx += 2 {
			if _, err := pipe.Set(ctx, kvs[idx], kvs[idx+1], options.TTL).Result(); err != nil {
				return err
			}
		}
		return nil
	})

	te := time.Since(ts)
	r.opts.Meter.Counter(semconv.CacheRequestInflight, "name", options.Name).Dec()
	r.opts.Meter.Summary(semconv.CacheRequestLatencyMicroseconds, "name", options.Name).Update(te.Seconds())
	r.opts.Meter.Histogram(semconv.CacheRequestDurationSeconds, "name", options.Name).Update(te.Seconds())
	if err == redis.Nil {
		r.opts.Meter.Counter(semconv.CacheRequestTotal, "name", options.Name, "status", "miss").Inc()
		return store.ErrNotFound
	} else if err == nil {
		r.opts.Meter.Counter(semconv.CacheRequestTotal, "name", options.Name, "status", "hit").Inc()
	} else if err != nil {
		sp.SetStatus(tracer.SpanStatusError, err.Error())
		r.opts.Meter.Counter(semconv.CacheRequestTotal, "name", options.Name, "status", "failure").Inc()
		return err
	}

	for _, cmd := range cmds {
		if err = cmd.Err(); err != nil {
			if err == redis.Nil {
				r.opts.Meter.Counter(semconv.CacheRequestTotal, "name", options.Name, "status", "miss").Inc()
				return store.ErrNotFound
			}
			sp.SetStatus(tracer.SpanStatusError, err.Error())
			r.opts.Meter.Counter(semconv.CacheRequestTotal, "name", options.Name, "status", "failure").Inc()
			return err
		}
	}

	return nil
}

func (r *Store) Write(ctx context.Context, key string, val interface{}, opts ...store.WriteOption) error {
	options := store.NewWriteOptions(opts...)

	timeout := r.opts.Timeout
	if options.Timeout > 0 {
		timeout = options.Timeout
	}

	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	rkey := r.getKey(r.opts.Namespace, options.Namespace, key)
	ctx, sp := r.opts.Tracer.Start(ctx, fmt.Sprintf("cache write %v", rkey))
	defer sp.Finish()

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
			sp.SetStatus(tracer.SpanStatusError, err.Error())
			return err
		}
	}

	r.opts.Meter.Counter(semconv.CacheRequestInflight, "name", options.Name).Inc()
	ts := time.Now()
	err := r.cli.Set(ctx, rkey, buf, options.TTL).Err()
	te := time.Since(ts)
	r.opts.Meter.Counter(semconv.CacheRequestInflight, "name", options.Name).Dec()
	r.opts.Meter.Summary(semconv.CacheRequestLatencyMicroseconds, "name", options.Name).Update(te.Seconds())
	r.opts.Meter.Histogram(semconv.CacheRequestDurationSeconds, "name", options.Name).Update(te.Seconds())
	if err == redis.Nil {
		r.opts.Meter.Counter(semconv.CacheRequestTotal, "name", options.Name, "status", "miss").Inc()
		return store.ErrNotFound
	} else if err == nil {
		r.opts.Meter.Counter(semconv.CacheRequestTotal, "name", options.Name, "status", "hit").Inc()
	} else if err != nil {
		sp.SetStatus(tracer.SpanStatusError, err.Error())
		r.opts.Meter.Counter(semconv.CacheRequestTotal, "name", options.Name, "status", "failure").Inc()
		return err
	}

	return err
}

func (r *Store) List(ctx context.Context, opts ...store.ListOption) ([]string, error) {
	options := store.NewListOptions(opts...)
	if len(options.Namespace) == 0 {
		options.Namespace = r.opts.Namespace
	}

	rkey := r.getKey(options.Namespace, "", options.Prefix+"*")
	if options.Suffix != "" {
		rkey += options.Suffix
	}

	timeout := r.opts.Timeout
	if options.Timeout > 0 {
		timeout = options.Timeout
	}

	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	ctx, sp := r.opts.Tracer.Start(ctx, fmt.Sprintf("cache list %v", rkey))
	defer sp.Finish()

	// TODO: add support for prefix/suffix/limit
	r.opts.Meter.Counter(semconv.CacheRequestInflight, "name", options.Name).Inc()
	ts := time.Now()
	keys, err := r.cli.Keys(ctx, rkey).Result()
	te := time.Since(ts)
	r.opts.Meter.Counter(semconv.CacheRequestInflight, "name", options.Name).Dec()
	r.opts.Meter.Summary(semconv.CacheRequestLatencyMicroseconds, "name", options.Name).Update(te.Seconds())
	r.opts.Meter.Histogram(semconv.CacheRequestDurationSeconds, "name", options.Name).Update(te.Seconds())
	if err == redis.Nil {
		r.opts.Meter.Counter(semconv.CacheRequestTotal, "name", options.Name, "status", "miss").Inc()
		return nil, store.ErrNotFound
	} else if err == nil {
		r.opts.Meter.Counter(semconv.CacheRequestTotal, "name", options.Name, "status", "hit").Inc()
	} else if err != nil {
		sp.SetStatus(tracer.SpanStatusError, err.Error())
		r.opts.Meter.Counter(semconv.CacheRequestTotal, "name", options.Name, "status", "failure").Inc()
		return nil, err
	}

	prefix := r.opts.Namespace
	if options.Namespace != "" {
		prefix = options.Namespace
	}
	if prefix == "" {
		return keys, nil
	}

	for idx, key := range keys {
		keys[idx] = strings.TrimPrefix(key, prefix)
	}

	return keys, nil
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
			redisOptions = DefaultOptions
			redisOptions.Addr = r.opts.Addrs[0]
			redisOptions.TLSConfig = r.opts.TLSConfig
		}
	} else if redisOptions == nil && redisClusterOptions == nil && len(nodes) > 1 {
		redisClusterOptions = DefaultClusterOptions
		redisClusterOptions.Addrs = r.opts.Addrs
		redisClusterOptions.TLSConfig = r.opts.TLSConfig
	}

	if redisOptions != nil {
		r.cli = redis.NewClient(redisOptions)
	} else if redisClusterOptions != nil {
		r.cli = redis.NewClusterClient(redisClusterOptions)
	}

	return nil
}

func (r *Store) getKey(mainNamespace string, opNamespace string, key string) string {
	b := r.pool.Get()
	defer r.pool.Put(b)
	b.Reset()

	if opNamespace == "" {
		opNamespace = mainNamespace
	}
	if opNamespace != "" {
		b.WriteString(opNamespace)
		b.WriteString(DefaultPathSeparator)
	}
	b.WriteString(key)
	return b.String()
}
