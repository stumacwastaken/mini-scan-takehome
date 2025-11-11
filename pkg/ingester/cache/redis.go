package cache

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/censys/scan-takehome/pkg/ingester"
	"github.com/redis/go-redis/v9"
)

// Cache when enabled will help determine if an incoming request is out of order.
// it will
type Cache struct {
	enabled bool
	rcl     redis.UniversalClient
	ttl     time.Duration
}

func NewCache(rcl redis.UniversalClient, ttl time.Duration) *Cache {
	return &Cache{
		enabled: true,
		rcl:     rcl,
		ttl:     ttl,
	}
}

func keyFromRecord(record ingester.Scan) string {
	return fmt.Sprintf("%s-%d-%s", record.Ip, record.Port, record.Service)
}

// RecordIsNew checks the incoming scan to see if the record is out of order. If it is not,
// it will update the cache with the newest timestamp.
func (c *Cache) RecordIsNew(ctx context.Context, record ingester.Scan) (bool, error) {
	key := keyFromRecord(ingester.Scan{})

	res, err := c.rcl.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			if _, err := c.rcl.Set(ctx, key, record.Timestamp, c.ttl).Result(); err != nil {
				return false, err
			}
			return true, nil
		}
		return false, err
	}

	unixTime, err := strconv.ParseInt(res, 10, 64)
	if err != nil {
		return false, err
	}

	// our record already exists and is newer. Move along.
	// This also stops us from needing to keep track of the identifiers
	if unixTime >= record.Timestamp {
		return false, nil
	}

	if _, err := c.rcl.Set(ctx, key, record.Timestamp, c.ttl).Result(); err != nil {
		return false, err
	}

	return false, nil
}

// In the event of a database failure, we should remove the records we've previously
// seen.
func (c *Cache) RemoveRecords(ctx context.Context, records []ingester.Scan) (int64, error) {
	var keys = make([]string, len(records))
	for i, record := range records {
		keys[i] = keyFromRecord(record)
	}

	res, err := c.rcl.Del(ctx, keys...).Result()
	return res, err

}
