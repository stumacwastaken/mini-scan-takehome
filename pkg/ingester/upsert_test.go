package ingester

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/censys/scan-takehome/pkg/scanning"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"
)

// General "unit" end to end test. Covers the start/end sequence with the ccontexts
// more tests should be added to cover the error case
func TestUpsertEndToEnd(t *testing.T) {
	tests := []struct {
		name             string
		mockErr          error
		expectedHitCount int // factor in the shutdown flush when using this
		flushInterval    time.Duration
		batchSize        int
		msgs             []Scan
	}{
		{
			name:             "positive: interval flush",
			expectedHitCount: 2,
			flushInterval:    time.Second * 1,
			batchSize:        10,
			msgs:             makeMessages(3),
		},
		{
			name:             "positive: batchsize flush",
			expectedHitCount: 3,
			flushInterval:    time.Second * 1,
			batchSize:        3,
			msgs:             makeMessages(5),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, fn := context.WithTimeout(context.Background(), tt.flushInterval+(time.Millisecond*500))
			var wg sync.WaitGroup
			l := zaptest.NewLogger(t)
			msgChan := make(chan *messageRequest)
			responseChan := make(chan messageResponse, len(tt.msgs))
			repo := &mockRepo{
				err: tt.mockErr,
				res: []Scan{},
			}
			subject := NewUpserter(l, msgChan, repo, tt.flushInterval, tt.batchSize)

			go subject.Start(ctx, &wg)
			wg.Add(1)
			for _, msg := range tt.msgs {
				req := &messageRequest{
					scan: msg,
					Res:  responseChan,
				}
				msgChan <- req
			}

			wg.Wait()
			assert.Equal(t, len(tt.msgs), len(responseChan), "expected msgs and msgs in response chan to be equal")
			assert.Equal(t, tt.expectedHitCount, repo.timesHit, "expected repo to be hit same number of times")
			assert.Equal(t, len(tt.msgs), len(repo.res), "expected repo store to be same as msg count")
			fn()
		})
	}
}

func TestCachePreventsOoOInsertions(t *testing.T) {
	attemptedMessageCount := 3
	savedMessageCount := 2
	messages := makeMessages(2)
	// create third timestamp that is significantly less than the others
	messages = append(messages, Scan{
		Scan: scanning.Scan{
			Ip:          "1.1.1.1",
			Port:        53,
			Service:     "DNS",
			Timestamp:   time.Now().Add(time.Second * -60).Unix(),
			DataVersion: 2,
			Data: scanning.V2Data{
				ResponseStr: "test-response",
			},
		}})

	ctx, fn := context.WithTimeout(context.Background(), (time.Second * 3))
	var wg sync.WaitGroup
	l := zaptest.NewLogger(t)
	msgChan := make(chan *messageRequest)
	responseChan := make(chan messageResponse, attemptedMessageCount)
	repo := &mockRepo{

		res: []Scan{},
	}
	subject := NewUpserter(l, msgChan, repo, time.Second*1, 20,
		WithCache(&mockCache{map[string]int64{}}),
	)

	go subject.Start(ctx, &wg)
	wg.Add(1)
	for _, msg := range messages {
		req := &messageRequest{
			scan: msg,
			Res:  responseChan,
		}
		msgChan <- req
	}

	wg.Wait()
	assert.Equal(t, attemptedMessageCount, len(responseChan), "expected msgs and msgs in response chan to be equal")
	assert.Equal(t, savedMessageCount, len(repo.res), "expected repo store to be same as msg count")
	fn()
}

func makeMessages(count int) []Scan {
	scans := []Scan{}
	for i := 0; i < count; i++ {
		scans = append(scans, Scan{
			Scan: scanning.Scan{
				Ip:          "1.1.1.1",
				Port:        53,
				Service:     "DNS",
				Timestamp:   time.Now().Add(time.Second * time.Duration(i)).Unix(),
				DataVersion: 2,
				Data: scanning.V2Data{
					ResponseStr: "test-response",
				},
			},
		})
	}
	return scans
}

type mockCache struct {
	cache map[string]int64
}

func (mc *mockCache) RemoveRecords(ctx context.Context, records []Scan) (int64, error) {
	mc.cache = map[string]int64{}
	return 0, nil
}
func (mc *mockCache) RecordIsNew(_ context.Context, record Scan) (bool, error) {
	key := fmt.Sprintf("%s-%d-%s", record.Ip, record.Port, record.Service)
	val, ok := mc.cache[key]
	if !ok {
		mc.cache[key] = record.Timestamp
		return true, nil
	}
	return record.Timestamp > val, nil
}

type mockRepo struct {
	err      error
	res      []Scan
	timesHit int
}

func (m *mockRepo) UpsertMany(_ context.Context, scans []Scan) error {
	if m.err != nil {
		return m.err
	}
	m.res = append(m.res, scans...)
	m.timesHit++
	return nil
}
