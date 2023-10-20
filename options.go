package redis

import (
	"github.com/redis/go-redis/v9"
	"go.unistack.org/micro/v4/options"
)

type configKey struct{}

func Config(c *redis.Options) options.Option {
	return options.ContextOption(configKey{}, c)
}

type clusterConfigKey struct{}

func ClusterConfig(c *redis.ClusterOptions) options.Option {
	return options.ContextOption(clusterConfigKey{}, c)
}
