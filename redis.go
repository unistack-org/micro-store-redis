package redis

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"go.unistack.org/micro/v3/semconv"
	"go.unistack.org/micro/v3/store"
	"go.unistack.org/micro/v3/util/id"
	pool "go.unistack.org/micro/v3/util/xpool"
)

var (
	_                       store.Store = (*Store)(nil)
	_                       store.Event = (*event)(nil)
	sendEventTime                       = 10 * time.Millisecond
	DefaultUniversalOptions             = &goredis.UniversalOptions{
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
	cli       goredis.UniversalClient
	pool      *pool.StringsPool
	connected *atomic.Uint32
	opts      store.Options
	watchers  map[string]*watcher
	mu        sync.RWMutex
}

func (r *Store) Live() bool {
	return r.connected.Load() == 1
}

func (r *Store) Ready() bool {
	return r.connected.Load() == 1
}

func (r *Store) Health() bool {
	return r.connected.Load() == 1
}

func (r *Store) Connect(ctx context.Context) error {
	if r.connected.Load() == 1 {
		return nil
	}
	if r.cli == nil {
		return store.ErrNotConnected
	}
	if r.opts.LazyConnect {
		return nil
	}
	if err := r.cli.Ping(ctx).Err(); err != nil {
		setSpanError(ctx, err)
		return err
	}
	r.connected.Store(1)
	return nil
}

func (r *Store) Init(opts ...store.Option) error {
	err := r.configure(opts...)
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
	if r.connected.Load() == 0 {
		return nil
	}

	if r.cli != nil {
		if err := r.cli.Close(); err != nil {
			return err
		}
	}

	r.connected.Store(1)
	return nil
}

func (r *Store) Exists(ctx context.Context, key string, opts ...store.ExistsOption) error {
	b := r.pool.Get()
	defer r.pool.Put(b)
	options := store.NewExistsOptions(opts...)
	labels := make([]string, 0, 6)
	labels = append(labels, "name", options.Name, "statement", "exists")

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

	r.opts.Meter.Counter(semconv.StoreRequestInflight, labels...).Inc()
	ts := time.Now()
	val, err := r.cli.Exists(ctx, rkey).Result()
	setSpanError(ctx, err)
	te := time.Since(ts)
	r.opts.Meter.Counter(semconv.StoreRequestInflight, labels...).Dec()

	r.opts.Meter.Summary(semconv.StoreRequestLatencyMicroseconds, labels...).Update(te.Seconds())
	r.opts.Meter.Histogram(semconv.StoreRequestDurationSeconds, labels...).Update(te.Seconds())
	if errors.Is(err, goredis.Nil) || (err == nil && val == 0) {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, append(labels, "status", "miss")...).Inc()
		return store.ErrNotFound
	} else if err == nil {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, append(labels, "status", "hit")...).Inc()
	} else {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, append(labels, "status", "failure")...).Inc()
		return err
	}

	return nil
}

func (r *Store) Read(ctx context.Context, key string, val interface{}, opts ...store.ReadOption) error {
	b := r.pool.Get()
	defer r.pool.Put(b)

	options := store.NewReadOptions(opts...)
	labels := make([]string, 0, 6)
	labels = append(labels, "name", options.Name, "statement", "read")

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

	r.opts.Meter.Counter(semconv.StoreRequestInflight, labels...).Inc()
	ts := time.Now()
	buf, err := r.cli.Get(ctx, rkey).Bytes()
	setSpanError(ctx, err)
	te := time.Since(ts)
	r.opts.Meter.Counter(semconv.StoreRequestInflight, labels...).Dec()
	r.opts.Meter.Summary(semconv.StoreRequestLatencyMicroseconds, labels...).Update(te.Seconds())
	r.opts.Meter.Histogram(semconv.StoreRequestDurationSeconds, labels...).Update(te.Seconds())
	if errors.Is(err, goredis.Nil) || (err == nil && buf == nil) {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, append(labels, "status", "miss")...).Inc()
		return store.ErrNotFound
	} else if err == nil {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, append(labels, "status", "hit")...).Inc()
	} else if err != nil {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, append(labels, "status", "failure")...).Inc()
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
	labels := make([]string, 0, 6)
	labels = append(labels, "name", options.Name, "statement", "delete")

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

	r.opts.Meter.Counter(semconv.StoreRequestInflight, labels...).Inc()
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
	r.opts.Meter.Counter(semconv.StoreRequestInflight, labels...).Dec()
	r.opts.Meter.Summary(semconv.StoreRequestLatencyMicroseconds, labels...).Update(te.Seconds())
	r.opts.Meter.Histogram(semconv.StoreRequestDurationSeconds, labels...).Update(te.Seconds())
	if err == goredis.Nil {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, append(labels, "status", "miss")...).Inc()
		return store.ErrNotFound
	} else if err == nil {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, append(labels, "status", "hit")...).Inc()
	} else if err != nil {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, append(labels, "status", "failure")...).Inc()
		return err
	}

	return nil
}

func (r *Store) Delete(ctx context.Context, key string, opts ...store.DeleteOption) error {
	b := r.pool.Get()
	defer r.pool.Put(b)

	options := store.NewDeleteOptions(opts...)
	labels := make([]string, 0, 6)
	labels = append(labels, "name", options.Name, "statement", "delete")

	timeout := r.opts.Timeout
	if options.Timeout > 0 {
		timeout = options.Timeout
	}

	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	r.opts.Meter.Counter(semconv.StoreRequestInflight, labels...).Inc()
	ts := time.Now()
	err := r.cli.Del(ctx, r.getKey(b, r.opts.Namespace, options.Namespace, key)).Err()
	te := time.Since(ts)
	setSpanError(ctx, err)

	r.opts.Meter.Counter(semconv.StoreRequestInflight, labels...).Dec()
	r.opts.Meter.Summary(semconv.StoreRequestLatencyMicroseconds, labels...).Update(te.Seconds())
	r.opts.Meter.Histogram(semconv.StoreRequestDurationSeconds, labels...).Update(te.Seconds())
	if errors.Is(err, goredis.Nil) {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, append(labels, "status", "miss")...).Inc()
		return store.ErrNotFound
	} else if err == nil {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, append(labels, "status", "hit")...).Inc()
	} else if err != nil {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, append(labels, "status", "failure")...).Inc()
		return err
	}

	return nil
}

func (r *Store) MWrite(ctx context.Context, keys []string, vals []interface{}, opts ...store.WriteOption) error {
	options := store.NewWriteOptions(opts...)
	labels := make([]string, 0, 6)
	labels = append(labels, "name", options.Name, "statement", "write")

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

	r.opts.Meter.Counter(semconv.StoreRequestInflight, labels...).Inc()

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

	r.opts.Meter.Counter(semconv.StoreRequestInflight, labels...).Dec()
	r.opts.Meter.Summary(semconv.StoreRequestLatencyMicroseconds, labels...).Update(te.Seconds())
	r.opts.Meter.Histogram(semconv.StoreRequestDurationSeconds, labels...).Update(te.Seconds())
	if err == goredis.Nil {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, append(labels, "status", "miss")...).Inc()
		return store.ErrNotFound
	} else if err == nil {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, append(labels, "status", "hit")...).Inc()
	} else if err != nil {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, append(labels, "status", "failure")...).Inc()
		return err
	}

	for _, cmd := range cmds {
		if err = cmd.Err(); err != nil {
			if err == goredis.Nil {
				r.opts.Meter.Counter(semconv.StoreRequestTotal, append(labels, "status", "miss")...).Inc()
				return store.ErrNotFound
			}
			setSpanError(ctx, err)
			r.opts.Meter.Counter(semconv.StoreRequestTotal, append(labels, "status", "failure")...).Inc()
			return err
		}
	}

	return nil
}

func (r *Store) Write(ctx context.Context, key string, val interface{}, opts ...store.WriteOption) error {
	b := r.pool.Get()
	defer r.pool.Put(b)

	options := store.NewWriteOptions(opts...)
	labels := make([]string, 0, 6)
	labels = append(labels, "name", options.Name, "statement", "write")

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

	r.opts.Meter.Counter(semconv.StoreRequestInflight, labels...).Inc()
	ts := time.Now()
	err := r.cli.Set(ctx, rkey, buf, options.TTL).Err()
	te := time.Since(ts)
	setSpanError(ctx, err)

	r.opts.Meter.Counter(semconv.StoreRequestInflight, labels...).Dec()
	r.opts.Meter.Summary(semconv.StoreRequestLatencyMicroseconds, labels...).Update(te.Seconds())
	r.opts.Meter.Histogram(semconv.StoreRequestDurationSeconds, labels...).Update(te.Seconds())
	if errors.Is(err, goredis.Nil) {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, append(labels, "status", "miss")...).Inc()
		return store.ErrNotFound
	} else if err == nil {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, append(labels, "status", "hit")...).Inc()
	} else {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, append(labels, "status", "failure")...).Inc()
		return err
	}

	return err
}

func (r *Store) List(ctx context.Context, opts ...store.ListOption) ([]string, error) {
	b := r.pool.Get()
	defer r.pool.Put(b)

	options := store.NewListOptions(opts...)
	labels := make([]string, 0, 6)
	labels = append(labels, "name", options.Name, "statement", "list")

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
	r.opts.Meter.Counter(semconv.StoreRequestInflight, labels...).Inc()
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

	r.opts.Meter.Counter(semconv.StoreRequestInflight, labels...).Dec()
	r.opts.Meter.Summary(semconv.StoreRequestLatencyMicroseconds, labels...).Update(te.Seconds())
	r.opts.Meter.Histogram(semconv.StoreRequestDurationSeconds, labels...).Update(te.Seconds())
	if err == goredis.Nil {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, append(labels, "status", "miss")...).Inc()
		return nil, store.ErrNotFound
	} else if err == nil {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, append(labels, "status", "git")...).Inc()
	} else if err != nil {
		r.opts.Meter.Counter(semconv.StoreRequestTotal, append(labels, "status", "failure")...).Inc()
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
	b := atomic.Uint32{}
	return &Store{
		opts:      store.NewOptions(opts...),
		connected: &b,
		watchers:  make(map[string]*watcher),
	}
}

func (r *Store) configure(opts ...store.Option) error {
	if r.cli != nil && len(opts) == 0 {
		return nil
	}

	for _, o := range opts {
		o(&r.opts)
	}

	universalOptions := DefaultUniversalOptions

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

			if o.TLSConfig != nil {
				universalOptions.TLSConfig = o.TLSConfig
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
			if o.TLSConfig != nil {
				universalOptions.TLSConfig = o.TLSConfig
			}
		}

		if o, ok := r.opts.Context.Value(universalConfigKey{}).(*goredis.UniversalOptions); ok {
			universalOptions = o
			if o.TLSConfig != nil {
				universalOptions.TLSConfig = o.TLSConfig
			}
		}
	}

	if len(r.opts.Addrs) > 0 {
		universalOptions.Addrs = r.opts.Addrs
	} else {
		universalOptions.Addrs = []string{"127.0.0.1:6379"}
	}

	r.cli = goredis.NewUniversalClient(universalOptions)
	setTracing(r.cli, r.opts.Tracer)
	r.cli.AddHook(newEventHook(r))

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
		b.WriteString(r.opts.Separator)
	}
	b.WriteString(key)
	return b.String()
}

func (r *Store) Watch(ctx context.Context, opts ...store.WatchOption) (store.Watcher, error) {
	id, err := id.New()
	if err != nil {
		return nil, err
	}
	wo, err := store.NewWatchOptions(opts...)
	if err != nil {
		return nil, err
	}
	// construct the watcher
	w := &watcher{
		exit: make(chan bool),
		ch:   make(chan store.Event),
		id:   id,
		opts: wo,
	}

	r.mu.Lock()
	r.watchers[w.id] = w
	r.mu.Unlock()

	return w, nil
}

func (r *Store) sendEvent(e store.Event) {
	r.mu.RLock()
	watchers := make([]*watcher, 0, len(r.watchers))
	for _, w := range r.watchers {
		watchers = append(watchers, w)
	}
	r.mu.RUnlock()
	for _, w := range watchers {
		select {
		case <-w.exit:
			r.mu.Lock()
			delete(r.watchers, w.id)
			r.mu.Unlock()
		default:
			select {
			case w.ch <- e:
			case <-time.After(sendEventTime):
			}
		}
	}
}

type watcher struct {
	ch   chan store.Event
	exit chan bool
	opts store.WatchOptions
	id   string
}

func (w *watcher) Next() (store.Event, error) {
	for {
		select {
		case e := <-w.ch:
			return e, nil
		case <-w.exit:
			return nil, store.ErrWatcherStopped
		}
	}
}

func (w *watcher) Stop() {
	select {
	case <-w.exit:
		return
	default:
		close(w.exit)
	}
}

type event struct {
	ts  time.Time
	t   store.EventType
	err error
}

func (e *event) Error() error {
	return e.err
}

func (e *event) Timestamp() time.Time {
	return e.ts
}

func (e *event) Type() store.EventType {
	return e.t
}
