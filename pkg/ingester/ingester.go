package ingester

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"cloud.google.com/go/pubsub"
	"go.uber.org/zap"
)

type messageResponse struct {
	err error
}

type messageRequest struct {
	scan Scan
	Res  chan messageResponse
}

// PubSubMessage is interfaced so we can pull out the logic into [ReceiveMessage]
// and test appropriately. This is a bit clunky in my opinion, but pstest wasn't
// playing nicely with my attempts to create a mock server (similar pattern to
// httptest)
type PubSubMessage interface {
	Ack()
	Nack()
}

// Ingester is the service that will listen on a pubsub subscription and
// orchestrate any work to additional processors. It will wait a default of
// 30 seconds for the upserter of a message to respond back with a successful
// insert. Otherwise it will send a Nack().
type Ingester struct {
	SendChan chan *messageRequest
	l        *zap.Logger
	sub      *pubsub.Subscription
	waitTime time.Duration
}

// IngesterOption provides additional configuration options for the ingester
type IngesterOption func(*Ingester)

// Overwrites the default sender channel to the Upserter in the event you
// need better coordination or visibility (and for a good test bailout)
func WithSendChannel(c chan *messageRequest) IngesterOption {
	return func(i *Ingester) {
		i.SendChan = c
	}
}

// WithMessageAckTimeout will configure the ingester to wait the duration provided
// before sending back a Nack() request to pubsub.
func WithMessageAckTimeout(t time.Duration) IngesterOption {
	return func(i *Ingester) {
		i.waitTime = t
	}
}

// NewIngester creates a new Ingester instance with the provided configurations
func NewIngester(l *zap.Logger, sub *pubsub.Subscription, opts ...IngesterOption) *Ingester {
	ingester := &Ingester{
		l:        l,
		sub:      sub,
		SendChan: make(chan *messageRequest),
		waitTime: time.Second * 30,
	}

	for _, opt := range opts {
		opt(ingester)
	}

	return ingester

}

// Start the ingester and listen for messages from the pubsub. It will then
// send the records to the Upserter for consideration
func (i *Ingester) Start(ctx context.Context) error {
	err := i.sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		i.receiveMessage(ctx, m.Data, m)

	})

	if err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

// keeping the context around in the event we ever wanted to do tracing or other
// things that would require a context.
func (i *Ingester) receiveMessage(_ context.Context, data []byte, m PubSubMessage) {
	i.l.Debug("received pubsub message")

	var msg Scan
	err := json.Unmarshal(data, &msg)
	if err != nil {
		i.l.Error("error unmarshaling!", zap.Error(err))
		return
	}

	doneChan := make(chan messageResponse)

	// this could be wrapped in a select statement with a timer if we wanted
	// more control on the Nack().
	i.SendChan <- &messageRequest{scan: msg, Res: doneChan}

	// I'm not sure how much of a loop we're in above us, but simply using time.After
	// _may_ lead to memory leaks, especially in versions of go  < 1.24
	ticker := time.NewTicker(i.waitTime)
	defer ticker.Stop()

	select {
	case res := <-doneChan:
		if res.err != nil {
			i.l.Error("failed to save message", zap.Error(err))
			// this would be a good place to have prometheus metrics so we could
			// see our rate of success vs failure on inserts, and the duration
			// it took to insert the message. Sadly I don't have the time to set
			// that up right now.
			m.Nack()
		} else {
			m.Ack()
		}

	case <-ticker.C:
		i.l.Debug("failed to save message")
		m.Nack()
	}
}
