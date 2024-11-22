package redis

import (
	"context"
	"fmt"
	"net"
	"strconv"

	rediscmd "github.com/redis/go-redis/extra/rediscmd/v9"
	goredis "github.com/redis/go-redis/v9"
	"go.unistack.org/micro/v3/tracer"
)

func setTracing(rdb goredis.UniversalClient, tr tracer.Tracer, opts ...tracer.SpanOption) {
	switch rdb := rdb.(type) {
	case *goredis.Client:
		opt := rdb.Options()
		connString := formatDBConnString(opt.Network, opt.Addr)
		rdb.AddHook(newTracingHook(connString, tr))
	case *goredis.ClusterClient:
		rdb.OnNewNode(func(rdb *goredis.Client) {
			opt := rdb.Options()
			connString := formatDBConnString(opt.Network, opt.Addr)
			rdb.AddHook(newTracingHook(connString, tr))
		})
	case *goredis.Ring:
		rdb.OnNewNode(func(rdb *goredis.Client) {
			opt := rdb.Options()
			connString := formatDBConnString(opt.Network, opt.Addr)
			rdb.AddHook(newTracingHook(connString, tr))
		})
	}
}

type tracingHook struct {
	tr   tracer.Tracer
	opts []tracer.SpanOption
}

var _ goredis.Hook = (*tracingHook)(nil)

func newTracingHook(connString string, tr tracer.Tracer, opts ...tracer.SpanOption) *tracingHook {
	opts = append(opts, tracer.WithSpanKind(tracer.SpanKindClient))
	if connString != "" {
		opts = append(opts, tracer.WithSpanLabels("db.connection_string", connString))
	}

	return &tracingHook{
		tr:   tr,
		opts: opts,
	}
}

func (h *tracingHook) DialHook(hook goredis.DialHook) goredis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		/*
			_, span := h.tr.Start(ctx, "goredis.dial", h.opts...)
			defer span.Finish()
		*/
		conn, err := hook(ctx, network, addr)
		// recordError(span, err)

		return conn, err
	}
}

func (h *tracingHook) ProcessHook(hook goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		cmdString := rediscmd.CmdString(cmd)
		var err error

		switch cmdString {
		case "cluster slots":
			break
		default:
			_, span := h.tr.Start(ctx, "sdk.database", append(h.opts, tracer.WithSpanLabels("db.statement", cmdString))...)
			defer func() {
				recordError(span, err)
				span.Finish()
			}()
		}

		err = hook(ctx, cmd)

		return err
	}
}

func (h *tracingHook) ProcessPipelineHook(hook goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []goredis.Cmder) error {
		_, cmdsString := rediscmd.CmdsString(cmds)

		opts := append(h.opts, tracer.WithSpanLabels(
			"db.database.num_cmd", strconv.Itoa(len(cmds)),
			"db.statement", cmdsString,
		))

		_, span := h.tr.Start(ctx, "sdk.database", opts...)
		defer span.Finish()

		err := hook(ctx, cmds)
		recordError(span, err)

		return err
	}
}

func setSpanError(ctx context.Context, err error) {
	if err == nil || err == goredis.Nil {
		return
	}
	if sp, ok := tracer.SpanFromContext(ctx); !ok && sp != nil {
		sp.SetStatus(tracer.SpanStatusError, err.Error())
	}
}

func recordError(span tracer.Span, err error) {
	if err != nil && err != goredis.Nil {
		span.SetStatus(tracer.SpanStatusError, err.Error())
	}
}

func formatDBConnString(network, addr string) string {
	if network == "tcp" {
		network = "redis"
	}
	return fmt.Sprintf("%s://%s", network, addr)
}
