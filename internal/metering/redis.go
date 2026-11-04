package metering

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// reserveScript atomically charges tokens to the tenant's current window,
// rejecting the charge when it would exceed the limit.
var reserveScript = redis.NewScript(`
local used  = tonumber(redis.call("GET", KEYS[1]) or "0")
local n     = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
if limit > 0 and used + n > limit then
    return -1
end
redis.call("INCRBY", KEYS[1], n)
redis.call("EXPIRE", KEYS[1], ARGV[3])
return used + n
`)

// commitScript adjusts the window by (actual - reserved), floored at zero.
var commitScript = redis.NewScript(`
local cur = tonumber(redis.call("GET", KEYS[1]) or "0")
local nxt = cur + tonumber(ARGV[1])
if nxt < 0 then
    nxt = 0
end
redis.call("SET", KEYS[1], tostring(nxt))
redis.call("EXPIRE", KEYS[1], ARGV[2])
return nxt
`)

// RedisBudgetStore keeps per-tenant window budgets in Redis so limits hold
// across gateway replicas. Keys are namespaced per window bucket and expire
// shortly after the bucket ends.
type RedisBudgetStore struct {
	client *redis.Client
	window time.Duration
	ttl    time.Duration
}

// NewRedisBudgetStore returns a Redis-backed store. Call Ping once at
// startup to fail fast on misconfiguration.
func NewRedisBudgetStore(addr, password string, window time.Duration) *RedisBudgetStore {
	if window <= 0 {
		window = 24 * time.Hour
	}
	return &RedisBudgetStore{
		client: redis.NewClient(&redis.Options{Addr: addr, Password: password}),
		window: window,
		ttl:    window + time.Minute,
	}
}

// Ping verifies connectivity.
func (s *RedisBudgetStore) Ping(ctx context.Context) error {
	return s.client.Ping(ctx).Err()
}

func (s *RedisBudgetStore) key(tenant string, now time.Time) string {
	return fmt.Sprintf("budget:%s:%d", tenant, windowBucket(now, s.window))
}

// Reserve charges tokens to the tenant's current window atomically.
func (s *RedisBudgetStore) Reserve(ctx context.Context, tenant string, tokens, limit int64) error {
	now := nowFunc()
	res, err := reserveScript.Run(ctx, s.client,
		[]string{s.key(tenant, now)},
		tokens, limit, int64(s.ttl.Seconds()),
	).Int64()
	if err != nil {
		return err
	}
	if res < 0 {
		return ErrBudgetExhausted
	}
	return nil
}

// Commit adjusts the tenant's current window by (actual - reserved).
func (s *RedisBudgetStore) Commit(ctx context.Context, tenant string, reserved, actual int64) error {
	now := nowFunc()
	_, err := commitScript.Run(ctx, s.client,
		[]string{s.key(tenant, now)},
		actual-reserved, int64(s.ttl.Seconds()),
	).Result()
	return err
}

// Close releases the Redis connection.
func (s *RedisBudgetStore) Close() error {
	return s.client.Close()
}
