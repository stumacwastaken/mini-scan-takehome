package cache

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/censys/scan-takehome/pkg/ingester"
	"github.com/censys/scan-takehome/pkg/scanning"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestCache(t *testing.T) {
	s := miniredis.RunT(t)
	rcl := redis.NewClient(&redis.Options{
		Addr:     s.Addr(),
		Password: "",
		DB:       0,
	})
	subject := NewCache(rcl, time.Hour*1)

	record := makeMessage(time.Now().Unix())
	isNew, err := subject.RecordIsNew(context.Background(), record)
	assert.NoError(t, err)
	assert.True(t, isNew)

	record2 := makeMessage(time.Now().Add(time.Hour * -1).Unix())
	isNew, err = subject.RecordIsNew(context.Background(), record2)
	assert.NoError(t, err)
	assert.False(t, isNew)

}

func makeMessage(timestamp int64) ingester.Scan {

	return ingester.Scan{
		Scan: scanning.Scan{
			Ip:          "1.1.1.1",
			Port:        53,
			Service:     "DNS",
			Timestamp:   timestamp,
			DataVersion: 2,
			Data: scanning.V2Data{
				ResponseStr: "test-response",
			},
		},
	}
}
