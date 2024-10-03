package redis

import (
	"github.com/redis/go-redis/v9"
	"go.unistack.org/micro/v4/options"
)

type universalConfigKey struct{}

func UniversalConfig(c *redis.UniversalOptions) options.Option {
	return options.ContextOption(universalConfigKey{}, c)
}
