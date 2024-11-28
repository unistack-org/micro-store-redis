package redis

import (
	"context"
	"errors"
	"net"
	"sync/atomic"

	goredis "github.com/redis/go-redis/v9"
)

type eventHook struct {
	connected *atomic.Bool
}

var _ goredis.Hook = (*eventHook)(nil)

func newEventHook(connected *atomic.Bool) *eventHook {
	return &eventHook{connected: connected}
}

func (h *eventHook) DialHook(hook goredis.DialHook) goredis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := hook(ctx, network, addr)
		if err != nil {
			if !isRedisError(err) {
				h.connected.Store(false)
			}
			h.connected.Store(true)
		} else {
			h.connected.Store(true)
		}
		return conn, err
	}
}

func (h *eventHook) ProcessHook(hook goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		err := hook(ctx, cmd)
		if err != nil && !isRedisError(err) {
			h.connected.Store(false)
		}
		return err
	}
}

func (h *eventHook) ProcessPipelineHook(hook goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []goredis.Cmder) error {
		err := hook(ctx, cmds)
		if err != nil && !isRedisError(err) {
			h.connected.Store(false)
		}
		return err
	}
}

func isRedisError(err error) bool {
	var rerr goredis.Error
	return errors.As(err, &rerr)
}
