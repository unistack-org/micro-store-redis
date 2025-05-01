package redis

import (
	"time"

	goredis "github.com/redis/go-redis/v9"
	"go.unistack.org/micro/v3/meter"
)

var (
	PoolHitsTotal        = "pool_hits_total"
	PoolMissesTotal      = "pool_misses_total"
	PoolTimeoutTotal     = "pool_timeout_total"
	PoolConnTotalCurrent = "pool_conn_total_current"
	PoolConnIdleCurrent  = "pool_conn_idle_current"
	PoolConnStaleTotal   = "pool_conn_stale_total"
)

type Statser interface {
	PoolStats() *goredis.PoolStats
}

func (r *Store) statsMeter() {
	var st Statser

	if r.cli != nil {
		st = r.cli
	} else {
		return
	}

	go func() {
		ticker := time.NewTicker(meter.DefaultMeterStatsInterval)
		defer ticker.Stop()

		for range ticker.C {
			if st == nil {
				return
			}
			stats := st.PoolStats()
			r.opts.Meter.Counter(PoolHitsTotal).Set(uint64(stats.Hits))
			r.opts.Meter.Counter(PoolMissesTotal).Set(uint64(stats.Misses))
			r.opts.Meter.Counter(PoolTimeoutTotal).Set(uint64(stats.Timeouts))
			r.opts.Meter.Counter(PoolConnTotalCurrent).Set(uint64(stats.TotalConns))
			r.opts.Meter.Counter(PoolConnIdleCurrent).Set(uint64(stats.IdleConns))
			r.opts.Meter.Counter(PoolConnStaleTotal).Set(uint64(stats.StaleConns))
		}
	}()
}
