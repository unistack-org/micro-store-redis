package redis

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"github.com/redis/go-redis/extra/rediscmd/v9"
	"github.com/redis/go-redis/v9"
	"go.unistack.org/micro/v3/tracer"
)

func setTracing(rdb redis.UniversalClient, tr tracer.Tracer, opts ...tracer.SpanOption) {
	switch rdb := rdb.(type) {
	case *redis.Client:
		opt := rdb.Options()
		connString := formatDBConnString(opt.Network, opt.Addr)
		opts = addServerAttributes(opts, opt.Addr)
		rdb.AddHook(newTracingHook(connString, tr, opts...))
	case *redis.ClusterClient:
		rdb.AddHook(newTracingHook("", tr, opts...))
		rdb.OnNewNode(func(rdb *redis.Client) {
			opt := rdb.Options()
			opts = addServerAttributes(opts, opt.Addr)
			connString := formatDBConnString(opt.Network, opt.Addr)
			rdb.AddHook(newTracingHook(connString, tr, opts...))
		})
	case *redis.Ring:
		rdb.AddHook(newTracingHook("", tr, opts...))
		rdb.OnNewNode(func(rdb *redis.Client) {
			opt := rdb.Options()
			opts = addServerAttributes(opts, opt.Addr)
			connString := formatDBConnString(opt.Network, opt.Addr)
			rdb.AddHook(newTracingHook(connString, tr, opts...))
		})
	}
}

type tracingHook struct {
	tr   tracer.Tracer
	opts []tracer.SpanOption
}

var _ redis.Hook = (*tracingHook)(nil)

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

func (h *tracingHook) DialHook(hook redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		ctx, span := h.tr.Start(ctx, "redis.dial", h.opts...)
		defer span.Finish()

		conn, err := hook(ctx, network, addr)
		if err != nil {
			recordError(span, err)
			return nil, err
		}
		return conn, nil
	}
}

func (h *tracingHook) ProcessHook(hook redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		cmdString := rediscmd.CmdString(cmd)

		ctx, span := h.tr.Start(ctx, cmd.FullName(), append(h.opts, tracer.WithSpanLabels("db.statement", cmdString))...)
		defer span.Finish()

		if err := hook(ctx, cmd); err != nil {
			recordError(span, err)
			return err
		}
		return nil
	}
}

func (h *tracingHook) ProcessPipelineHook(
	hook redis.ProcessPipelineHook,
) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		summary, cmdsString := rediscmd.CmdsString(cmds)

		opts := append(h.opts, tracer.WithSpanLabels(
			"db.redis.num_cmd", strconv.Itoa(len(cmds)),
			"db.statement", cmdsString,
		))

		ctx, span := h.tr.Start(ctx, "redis.pipeline "+summary, opts...)
		defer span.Finish()

		if err := hook(ctx, cmds); err != nil {
			recordError(span, err)
			return err
		}
		return nil
	}
}

func recordError(span tracer.Span, err error) {
	if err != redis.Nil {
		span.SetStatus(tracer.SpanStatusError, err.Error())
	}
}

func formatDBConnString(network, addr string) string {
	if network == "tcp" {
		network = "redis"
	}
	return fmt.Sprintf("%s://%s", network, addr)
}

// Database span attributes semantic conventions recommended server address and port
// https://opentelemetry.io/docs/specs/semconv/database/database-spans/#connection-level-attributes
func addServerAttributes(opts []tracer.SpanOption, addr string) []tracer.SpanOption {
	host, portString, err := net.SplitHostPort(addr)
	if err != nil {
		return opts
	}

	opts = append(opts, tracer.WithSpanLabels("server.address", host))

	// Parse the port string to an integer
	port, err := strconv.Atoi(portString)
	if err != nil {
		return opts
	}

	opts = append(opts, tracer.WithSpanLabels("server.port", port))

	return opts
}
