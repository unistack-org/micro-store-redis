package redis

import (
	"github.com/redis/go-redis/v9"
	"go.unistack.org/micro/v3/store"
)

type configKey struct{}

func Config(c *redis.Options) store.Option {
	return store.SetOption(configKey{}, c)
}

type clusterConfigKey struct{}

func ClusterConfig(c *redis.ClusterOptions) store.Option {
	return store.SetOption(clusterConfigKey{}, c)
}
