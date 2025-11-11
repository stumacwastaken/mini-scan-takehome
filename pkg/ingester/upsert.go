package ingester

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
)

type UpsertRepository interface {
	UpsertMany(ctx context.Context, scans []Scan) error
}

type RecordCache interface {
	RecordIsNew(ctx context.Context, record Scan) (bool, error)
	RemoveRecords(ctx context.Context, records []Scan) (int64, error)
}

type Upserter struct {
	FlushInterval time.Duration
	BatchSize     int
	MsgChan       chan *messageRequest
	Repository    UpsertRepository
	Log           *zap.Logger
	// keep the batch private so we can move it to a different structure (or service)
	batch []*messageRequest
	mu    sync.Mutex
	cache RecordCache
}

type UpserterOption func(u *Upserter)

func WithCache(c RecordCache) UpserterOption {
	return func(u *Upserter) {
		u.cache = c
	}
}

// NewUpserter will create a new Upserter service struct.
func NewUpserter(
	log *zap.Logger,
	msgChan chan *messageRequest,
	repo UpsertRepository,
	flushInterval time.Duration,
	batchSize int,
	opts ...UpserterOption) *Upserter {
	up := &Upserter{
		Log:           log,
		MsgChan:       msgChan,
		BatchSize:     batchSize,
		batch:         []*messageRequest{},
		FlushInterval: flushInterval,
		Repository:    repo,
	}
	for _, opt := range opts {
		opt(up)
	}
	if up.cache == nil {
		up.Log.Info("No cache is set. Relying on database for ordering")
	}
	return up
}

// Start will have the upserter start listening on the provided [MsgChan]. It will
// take any message and store it for processing later. To stop the listener cancel
// or finish the provided context
func (u *Upserter) Start(ctx context.Context, wg *sync.WaitGroup) error {
	go func() {
		err := u.flushWithInterval(ctx)
		if err != nil {
			u.Log.Error("Error attempting to flush on interval", zap.Error(err))
		}
	}()

	for {
		select {
		case <-ctx.Done():
			u.Log.Debug("context ended. Exiting")
			u.Flush()
			wg.Done()
			return nil
		case msg := <-u.MsgChan:
			if !u.shouldContinueProcessing(ctx, msg.scan) {
				msg.Res <- messageResponse{}
				continue
			}
			u.mu.Lock()
			u.batch = append(u.batch, msg)
			u.mu.Unlock()
			if len(u.batch) > u.BatchSize {
				u.Flush()
				// technically we aren't restarting the timer here, but it's not a bad idea.
			}
		}
	}
}

// Flush will attempt to take all records the upserter has and save them to the data store
func (u *Upserter) Flush() {
	// hardcoding a 60 second timeout. In a better work this would be configurable.
	ctx, fn := context.WithTimeout(context.Background(), time.Second*60)
	defer fn()
	u.Log.Debug("flushing entries to data store", zap.Int("count", len(u.batch)))
	u.mu.Lock()
	copiedMessages := make([]*messageRequest, len(u.batch))
	copy(copiedMessages, u.batch)
	u.batch = u.batch[:0]
	u.mu.Unlock()

	records := make([]Scan, len(copiedMessages))
	for i, msg := range copiedMessages {
		records[i] = msg.scan

	}

	// UpsertMany is transactional, so this is an all or nothing deal
	// The tradeoff means that any records that would've been ack'd are
	// going to get nack'd and we'll need to accept that they'll come again
	// later.
	err := u.Repository.UpsertMany(ctx, records)
	if err != nil {
		u.Log.Error("error flushing records to data store!", zap.Error(err))
		u.Log.Warn("removing redis keys that previously existed")
		if res, err := u.cache.RemoveRecords(ctx, records); err != nil {
			u.Log.Error("error removing records from cache", zap.Error(err))
		} else {
			u.Log.Debug("removed records from cache", zap.Int64("count", res))
		}
	}

	res := messageResponse{
		err: err,
	}

	for _, msg := range copiedMessages {
		msg.Res <- res
	}

}

func (u *Upserter) flushWithInterval(ctx context.Context) error {
	ticker := time.NewTicker(u.FlushInterval)
	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			// don't worry about trying to flush here, let the Start() process handle it
		case <-ticker.C:
			u.Log.Debug("flushing records to store")
			u.Flush()
		}
	}
}

func (u *Upserter) shouldContinueProcessing(ctx context.Context, record Scan) bool {
	if u.cache == nil {
		u.Log.Debug("cache not enabled")
		return true
	}
	ok, err := u.cache.RecordIsNew(ctx, record)
	if err != nil {
		u.Log.Error("error attempting to retreive cache record, processing as if it isn't seen", zap.Error(err))
		return true
	}

	return ok

}
