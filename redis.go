package redis

import (
	"context"
	"reflect"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"go.unistack.org/micro/v3/semconv"
	"go.unistack.org/micro/v3/store"
	pool "go.unistack.org/micro/v3/util/xpool"
)

var (
	DefaultPathSeparator = "/"

	DefaultUniversalOptions = &goredis.UniversalOptions{
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

	DefaultClusterOptions = &goredis.ClusterOptions{
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

	DefaultOptions = &goredis.Options{
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
	cli  goredis.UniversalClient
	done chan struct{}
	pool *pool.StringsPool
}

func (r *Store) Connect(ctx context.Context) error {
	if r.cli == nil {
		return store.ErrNotConnected
	}
	err := r.cli.Ping(ctx).Err()
	setSpanError(ctx, err)
	return err
}

func (r *Store) Init(opts ...store.Option) error {
	for _, o := range opts {
		o(&r.opts)
	}

	err := r.configure()
	if err != nil {
		return err
	}

	return nil
}

func (r *Store) Client() *goredis.Client {
	if c, ok := r.cli.(*goredis.Client); ok {
		return c
	}
	return nil
}

func (r *Store) UniversalClient() goredis.UniversalClient {
	return r.cli
}

func (r *Store) ClusterClient() *goredis.ClusterClient {
	if c, ok := r.cli.(*goredis.ClusterClient); ok {
		return c
	}
	return nil
}

func (r *Store) Disconnect(ctx context.Context) error {
	var err error
	select {
	case <-r.done:
		return err
	default:
		if r.cli != nil {
			err = r.cli.Close()
		}
		close(r.done)
		return err
	}
}

func (r *Store) Exists(ctx context.Context, key string, opts ...store.ExistsOption) error {
	b := r.pool.Get()
	defer r.pool.Put(b)
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

	rkey := r.getKey(b, r.opts.Namespace, options.Namespace, key)

	r.opts.Meter.Counter(semconv.StoreRequestInflight, "name", options.Name).Inc()
	ts := time.Now()
	val, err := r.cli.Exists(ctx, rkey).Result()
	setSpanError(ctx, err)
	te := time.Since(ts)
	r.opts.Meter.Counter(semconv.StoreRequestInflight, "name", options.Name).Dec()
	r.opts.Meter.Summary(semconv.StoreRequestLatencyMicroseconds, "name", options.Name).Update(te.Seconds())
	r.opts.Meter.Histogram(semconv.StoreRequestDurationSeconds, "name", options.Name).Update(te.Seconds())
	if err == goredis.Nil || (err == nil && val == 0) {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, "name", options.Name, "status", "miss").Inc()
		return store.ErrNotFound
	} else if err == nil {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, "name", options.Name, "status", "hit").Inc()
	} else if err != nil {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, "name", options.Name, "status", "failure").Inc()
		return err
	}

	return nil
}

func (r *Store) Read(ctx context.Context, key string, val interface{}, opts ...store.ReadOption) error {
	b := r.pool.Get()
	defer r.pool.Put(b)

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

	rkey := r.getKey(b, r.opts.Namespace, options.Namespace, key)

	r.opts.Meter.Counter(semconv.StoreRequestInflight, "name", options.Name).Inc()
	ts := time.Now()
	buf, err := r.cli.Get(ctx, rkey).Bytes()
	setSpanError(ctx, err)
	te := time.Since(ts)
	r.opts.Meter.Counter(semconv.StoreRequestInflight, "name", options.Name).Dec()
	r.opts.Meter.Summary(semconv.StoreRequestLatencyMicroseconds, "name", options.Name).Update(te.Seconds())
	r.opts.Meter.Histogram(semconv.StoreRequestDurationSeconds, "name", options.Name).Update(te.Seconds())
	if err == goredis.Nil || (err == nil && buf == nil) {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, "name", options.Name, "status", "miss").Inc()
		return store.ErrNotFound
	} else if err == nil {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, "name", options.Name, "status", "hit").Inc()
	} else if err != nil {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, "name", options.Name, "status", "failure").Inc()
		return err
	}

	switch b := val.(type) {
	case *[]byte:
		*b = buf
	case *string:
		*b = string(buf)
	default:
		if err = r.opts.Codec.Unmarshal(buf, val); err != nil {
			setSpanError(ctx, err)
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

	var rkeys []string
	var pools []*strings.Builder
	if r.opts.Namespace != "" || options.Namespace != "" {
		rkeys = make([]string, len(keys))
		pools = make([]*strings.Builder, len(keys))
		for idx, key := range keys {
			b := r.pool.Get()
			pools[idx] = b
			rkeys[idx] = r.getKey(b, r.opts.Namespace, options.Namespace, key)
		}
	}

	r.opts.Meter.Counter(semconv.StoreRequestInflight, "name", options.Name).Inc()
	ts := time.Now()
	var rvals []interface{}
	var err error
	if r.opts.Namespace != "" || options.Namespace != "" {
		rvals, err = r.cli.MGet(ctx, rkeys...).Result()
		for idx := range pools {
			r.pool.Put(pools[idx])
		}
	} else {
		rvals, err = r.cli.MGet(ctx, keys...).Result()
	}
	setSpanError(ctx, err)
	te := time.Since(ts)
	r.opts.Meter.Counter(semconv.StoreRequestInflight, "name", options.Name).Dec()
	r.opts.Meter.Summary(semconv.StoreRequestLatencyMicroseconds, "name", options.Name).Update(te.Seconds())
	r.opts.Meter.Histogram(semconv.StoreRequestDurationSeconds, "name", options.Name).Update(te.Seconds())
	if err == goredis.Nil || (len(rvals) == 0) {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, "name", options.Name, "status", "miss").Inc()
		return store.ErrNotFound
	} else if err == nil {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, "name", options.Name, "status", "hit").Inc()
	} else if err != nil {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, "name", options.Name, "status", "failure").Inc()
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
			setSpanError(ctx, err)
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

	var rkeys []string
	var pools []*strings.Builder
	if r.opts.Namespace != "" || options.Namespace != "" {
		rkeys = make([]string, len(keys))
		pools = make([]*strings.Builder, len(keys))
		for idx, key := range keys {
			b := r.pool.Get()
			pools[idx] = b
			rkeys[idx] = r.getKey(b, r.opts.Namespace, options.Namespace, key)
		}
	}

	r.opts.Meter.Counter(semconv.StoreRequestInflight, "name", options.Name).Inc()
	ts := time.Now()
	var err error
	if r.opts.Namespace != "" || options.Namespace != "" {
		err = r.cli.Del(ctx, rkeys...).Err()
		for idx := range pools {
			r.pool.Put(pools[idx])
		}
	} else {
		err = r.cli.Del(ctx, keys...).Err()
	}
	setSpanError(ctx, err)
	te := time.Since(ts)
	r.opts.Meter.Counter(semconv.StoreRequestInflight, "name", options.Name).Dec()
	r.opts.Meter.Summary(semconv.StoreRequestLatencyMicroseconds, "name", options.Name).Update(te.Seconds())
	r.opts.Meter.Histogram(semconv.StoreRequestDurationSeconds, "name", options.Name).Update(te.Seconds())
	if err == goredis.Nil {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, "name", options.Name, "status", "miss").Inc()
		return store.ErrNotFound
	} else if err == nil {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, "name", options.Name, "status", "hit").Inc()
	} else if err != nil {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, "name", options.Name, "status", "failure").Inc()
		return err
	}

	return nil
}

func (r *Store) Delete(ctx context.Context, key string, opts ...store.DeleteOption) error {
	b := r.pool.Get()
	defer r.pool.Put(b)

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

	r.opts.Meter.Counter(semconv.StoreRequestInflight, "name", options.Name).Inc()
	ts := time.Now()
	err := r.cli.Del(ctx, r.getKey(b, r.opts.Namespace, options.Namespace, key)).Err()
	te := time.Since(ts)
	setSpanError(ctx, err)
	r.opts.Meter.Counter(semconv.StoreRequestInflight, "name", options.Name).Dec()
	r.opts.Meter.Summary(semconv.StoreRequestLatencyMicroseconds, "name", options.Name).Update(te.Seconds())
	r.opts.Meter.Histogram(semconv.StoreRequestDurationSeconds, "name", options.Name).Update(te.Seconds())
	if err == goredis.Nil {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, "name", options.Name, "status", "miss").Inc()
		return store.ErrNotFound
	} else if err == nil {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, "name", options.Name, "status", "hit").Inc()
	} else if err != nil {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, "name", options.Name, "status", "failure").Inc()
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

	kvs := make([]string, 0, len(keys)*2)
	pools := make([]*strings.Builder, len(keys))
	for idx, key := range keys {
		b := r.pool.Get()
		pools[idx] = b
		kvs = append(kvs, r.getKey(b, r.opts.Namespace, options.Namespace, key))

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

	r.opts.Meter.Counter(semconv.StoreRequestInflight, "name", options.Name).Inc()

	pipeliner := func(pipe goredis.Pipeliner) error {
		for idx := 0; idx < len(kvs); idx += 2 {
			if _, err := pipe.Set(ctx, kvs[idx], kvs[idx+1], options.TTL).Result(); err != nil {
				setSpanError(ctx, err)
				return err
			}
		}
		return nil
	}

	ts := time.Now()
	cmds, err := r.cli.Pipelined(ctx, pipeliner)
	for idx := range pools {
		r.pool.Put(pools[idx])
	}
	te := time.Since(ts)
	setSpanError(ctx, err)

	r.opts.Meter.Counter(semconv.StoreRequestInflight, "name", options.Name).Dec()
	r.opts.Meter.Summary(semconv.StoreRequestLatencyMicroseconds, "name", options.Name).Update(te.Seconds())
	r.opts.Meter.Histogram(semconv.StoreRequestDurationSeconds, "name", options.Name).Update(te.Seconds())
	if err == goredis.Nil {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, "name", options.Name, "status", "miss").Inc()
		return store.ErrNotFound
	} else if err == nil {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, "name", options.Name, "status", "hit").Inc()
	} else if err != nil {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, "name", options.Name, "status", "failure").Inc()
		return err
	}

	for _, cmd := range cmds {
		if err = cmd.Err(); err != nil {
			if err == goredis.Nil {
				r.opts.Meter.Counter(semconv.StoreRequestTotal, "name", options.Name, "status", "miss").Inc()
				return store.ErrNotFound
			}
			setSpanError(ctx, err)
			r.opts.Meter.Counter(semconv.StoreRequestTotal, "name", options.Name, "status", "failure").Inc()
			return err
		}
	}

	return nil
}

func (r *Store) Write(ctx context.Context, key string, val interface{}, opts ...store.WriteOption) error {
	b := r.pool.Get()
	defer r.pool.Put(b)

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

	rkey := r.getKey(b, r.opts.Namespace, options.Namespace, key)

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

	r.opts.Meter.Counter(semconv.StoreRequestInflight, "name", options.Name).Inc()
	ts := time.Now()
	err := r.cli.Set(ctx, rkey, buf, options.TTL).Err()
	te := time.Since(ts)
	setSpanError(ctx, err)

	r.opts.Meter.Counter(semconv.StoreRequestInflight, "name", options.Name).Dec()
	r.opts.Meter.Summary(semconv.StoreRequestLatencyMicroseconds, "name", options.Name).Update(te.Seconds())
	r.opts.Meter.Histogram(semconv.StoreRequestDurationSeconds, "name", options.Name).Update(te.Seconds())
	if err == goredis.Nil {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, "name", options.Name, "status", "miss").Inc()
		return store.ErrNotFound
	} else if err == nil {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, "name", options.Name, "status", "hit").Inc()
	} else if err != nil {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, "name", options.Name, "status", "failure").Inc()
		return err
	}

	return err
}

func (r *Store) List(ctx context.Context, opts ...store.ListOption) ([]string, error) {
	b := r.pool.Get()
	defer r.pool.Put(b)

	options := store.NewListOptions(opts...)
	if len(options.Namespace) == 0 {
		options.Namespace = r.opts.Namespace
	}

	rkey := r.getKey(b, options.Namespace, "", options.Prefix+"*")
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

	// TODO: add support for prefix/suffix/limit
	r.opts.Meter.Counter(semconv.StoreRequestInflight, "name", options.Name).Inc()
	ts := time.Now()
	var keys []string
	var err error

	if c, ok := r.cli.(*goredis.ClusterClient); ok {
		err = c.ForEachMaster(ctx, func(nctx context.Context, cli *goredis.Client) error {
			nkeys, nerr := cli.Keys(nctx, rkey).Result()
			if nerr != nil {
				return nerr
			}
			keys = append(keys, nkeys...)
			return nil
		})
	} else {
		keys, err = r.cli.Keys(ctx, rkey).Result()
	}
	te := time.Since(ts)
	setSpanError(ctx, err)

	r.opts.Meter.Counter(semconv.StoreRequestInflight, "name", options.Name).Dec()
	r.opts.Meter.Summary(semconv.StoreRequestLatencyMicroseconds, "name", options.Name).Update(te.Seconds())
	r.opts.Meter.Histogram(semconv.StoreRequestDurationSeconds, "name", options.Name).Update(te.Seconds())
	if err == goredis.Nil {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, "name", options.Name, "status", "miss").Inc()
		return nil, store.ErrNotFound
	} else if err == nil {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, "name", options.Name, "status", "hit").Inc()
	} else if err != nil {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, "name", options.Name, "status", "failure").Inc()
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
	return &Store{done: make(chan struct{}), opts: store.NewOptions(opts...)}
}

func (r *Store) configure() error {
	universalOptions := &goredis.UniversalOptions{}

	if r.cli != nil && r.opts.Context == nil {
		return nil
	}

	if r.opts.Context != nil {
		if o, ok := r.opts.Context.Value(configKey{}).(*goredis.Options); ok {
			universalOptions.Addrs = []string{o.Addr}
			universalOptions.Dialer = o.Dialer
			universalOptions.OnConnect = o.OnConnect
			universalOptions.Username = o.Username
			universalOptions.Password = o.Password

			universalOptions.MaxRetries = o.MaxRetries
			universalOptions.MinRetryBackoff = o.MinRetryBackoff
			universalOptions.MaxRetryBackoff = o.MaxRetryBackoff

			universalOptions.DialTimeout = o.DialTimeout
			universalOptions.ReadTimeout = o.ReadTimeout
			universalOptions.WriteTimeout = o.WriteTimeout
			universalOptions.ContextTimeoutEnabled = o.ContextTimeoutEnabled

			universalOptions.PoolFIFO = o.PoolFIFO

			universalOptions.PoolSize = o.PoolSize
			universalOptions.PoolTimeout = o.PoolTimeout
			universalOptions.MinIdleConns = o.MinIdleConns
			universalOptions.MaxIdleConns = o.MaxIdleConns
			universalOptions.ConnMaxIdleTime = o.ConnMaxIdleTime
			universalOptions.ConnMaxLifetime = o.ConnMaxLifetime

			if r.opts.TLSConfig != nil {
				universalOptions.TLSConfig = r.opts.TLSConfig
			}
		}

		if o, ok := r.opts.Context.Value(clusterConfigKey{}).(*goredis.ClusterOptions); ok {
			universalOptions.Addrs = o.Addrs
			universalOptions.Dialer = o.Dialer
			universalOptions.OnConnect = o.OnConnect
			universalOptions.Username = o.Username
			universalOptions.Password = o.Password

			universalOptions.MaxRedirects = o.MaxRedirects
			universalOptions.ReadOnly = o.ReadOnly
			universalOptions.RouteByLatency = o.RouteByLatency
			universalOptions.RouteRandomly = o.RouteRandomly

			universalOptions.MaxRetries = o.MaxRetries
			universalOptions.MinRetryBackoff = o.MinRetryBackoff
			universalOptions.MaxRetryBackoff = o.MaxRetryBackoff

			universalOptions.DialTimeout = o.DialTimeout
			universalOptions.ReadTimeout = o.ReadTimeout
			universalOptions.WriteTimeout = o.WriteTimeout
			universalOptions.ContextTimeoutEnabled = o.ContextTimeoutEnabled

			universalOptions.PoolFIFO = o.PoolFIFO

			universalOptions.PoolSize = o.PoolSize
			universalOptions.PoolTimeout = o.PoolTimeout
			universalOptions.MinIdleConns = o.MinIdleConns
			universalOptions.MaxIdleConns = o.MaxIdleConns
			universalOptions.ConnMaxIdleTime = o.ConnMaxIdleTime
			universalOptions.ConnMaxLifetime = o.ConnMaxLifetime
			if r.opts.TLSConfig != nil {
				universalOptions.TLSConfig = r.opts.TLSConfig
			}
		}

		if o, ok := r.opts.Context.Value(universalConfigKey{}).(*goredis.UniversalOptions); ok {
			universalOptions = o
			if r.opts.TLSConfig != nil {
				universalOptions.TLSConfig = r.opts.TLSConfig
			}
		}
	}

	if universalOptions == nil {
		universalOptions = DefaultUniversalOptions
	}

	if len(r.opts.Addrs) > 0 {
		universalOptions.Addrs = r.opts.Addrs
	} else {
		universalOptions.Addrs = []string{"127.0.0.1:6379"}
	}

	r.cli = goredis.NewUniversalClient(universalOptions)
	setTracing(r.cli, r.opts.Tracer)

	r.pool = pool.NewStringsPool(50)

	r.statsMeter()

	return nil
}

func (r *Store) getKey(b *strings.Builder, mainNamespace string, opNamespace string, key string) string {
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
