package redis

import (
	goredis "github.com/redis/go-redis/v9"
	"go.unistack.org/micro/v3/logger"
	"go.unistack.org/micro/v3/meter"
	"go.unistack.org/micro/v3/store"
	"go.unistack.org/micro/v3/tracer"
)

type configKey struct{}

func Config(c *goredis.Options) store.Option {
	return store.SetOption(configKey{}, c)
}

type clusterConfigKey struct{}

func ClusterConfig(c *goredis.ClusterOptions) store.Option {
	return store.SetOption(clusterConfigKey{}, c)
}

type universalConfigKey struct{}

func UniversalConfig(c *goredis.UniversalOptions) store.Option {
	return store.SetOption(universalConfigKey{}, c)
}

var (
	labelHost = "redis_host"
	labelName = "redis_name"
)

// Options struct holds wrapper options
type Options struct {
	Logger    logger.Logger
	Meter     meter.Meter
	Tracer    tracer.Tracer
	RedisHost string
	RedisName string
}

// Option func signature
type Option func(*Options)

// NewOptions create new Options struct from provided option slice
func NewOptions(opts ...Option) Options {
	options := Options{
		Logger: logger.DefaultLogger,
		Meter:  meter.DefaultMeter,
		Tracer: tracer.DefaultTracer,
	}

	for _, o := range opts {
		o(&options)
	}

	options.Meter = options.Meter.Clone(
		meter.Labels(
			labelHost, options.RedisHost,
			labelName, options.RedisName),
	)

	options.Logger = options.Logger.Clone(logger.WithCallerSkipCount(1))

	return options
}
