package redis

import (
	"github.com/redis/go-redis/v9"
	"go.unistack.org/micro/v4/store"
)

type universalConfigKey struct{}

func UniversalConfig(c *redis.UniversalOptions) store.Option {
	return store.SetOption(universalConfigKey{}, c)
}
