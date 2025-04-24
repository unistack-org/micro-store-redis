package redis

import (
	"context"
	"errors"
	"net"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"go.unistack.org/micro/v4/store"
)

type eventHook struct {
	s *Store
}

var _ goredis.Hook = (*eventHook)(nil)

func newEventHook(s *Store) *eventHook {
	return &eventHook{s: s}
}

func (h *eventHook) DialHook(hook goredis.DialHook) goredis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := hook(ctx, network, addr)
		if err != nil {
			if !isRedisError(err) {
				if h.s.connected.CompareAndSwap(1, 0) {
					h.s.sendEvent(&event{ts: time.Now(), err: err, t: store.EventTypeDisconnect})
				}
			} else {
				h.s.connected.Store(1)
			}
		} else {
			if h.s.connected.CompareAndSwap(0, 1) {
				h.s.sendEvent(&event{ts: time.Now(), err: err, t: store.EventTypeConnect})
			}
		}
		return conn, err
	}
}

func (h *eventHook) ProcessHook(hook goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		err := hook(ctx, cmd)
		if err != nil {
			if !isRedisError(err) {
				if h.s.connected.CompareAndSwap(1, 0) {
					h.s.sendEvent(&event{ts: time.Now(), err: err, t: store.EventTypeDisconnect})
				}
			} else {
				h.s.connected.Store(1)
			}
		} else {
			if h.s.connected.CompareAndSwap(0, 1) {
				h.s.sendEvent(&event{ts: time.Now(), err: err, t: store.EventTypeConnect})
			}
		}
		return err
	}
}

func (h *eventHook) ProcessPipelineHook(hook goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []goredis.Cmder) error {
		err := hook(ctx, cmds)
		if err != nil {
			if !isRedisError(err) {
				if h.s.connected.CompareAndSwap(1, 0) {
					h.s.sendEvent(&event{ts: time.Now(), err: err, t: store.EventTypeDisconnect})
				}
			} else {
				h.s.connected.Store(1)
			}
		} else {
			if h.s.connected.CompareAndSwap(0, 1) {
				h.s.sendEvent(&event{ts: time.Now(), err: err, t: store.EventTypeConnect})
			}
		}
		return err
	}
}

func isRedisError(err error) bool {
	var rerr goredis.Error
	return errors.As(err, &rerr)
}
