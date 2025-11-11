package ingester

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/censys/scan-takehome/pkg/scanning"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"
)

// Because we're using the Receive function in Start() and it doesn't
// necessarily make sense to pull out that functionality into an interface
// because the core logic is in [receiveMessage] we'll instead use that instead.
// [Start] can be tested with integration tests. You can make the argument that
// the receiveMessage should be public, but given how tied it is to google's
// pubsub I'm ok leaving it for now.
func TestReceiveMessage(t *testing.T) {

	tests := []struct {
		name        string
		shouldAck   bool
		shouldNack  bool
		dataVersion int
	}{
		{
			name:        "success: version 1 should ack",
			shouldAck:   true,
			dataVersion: 1,
		},
		{
			name:        "success: version 2 should ack",
			shouldAck:   true,
			dataVersion: 2,
		},
		{
			name:        "failure, should nack",
			shouldNack:  true,
			dataVersion: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, fn := context.WithTimeout(context.Background(), time.Second*5)
			l := zaptest.NewLogger(t)

			subject := NewIngester(l, nil)

			s := newScan(tt.dataVersion)
			mm := newMockMsg()
			// this method will block if no one is on the line, or if the channel
			// isn't buffered. So keep it in its own goroutine for tests
			go subject.receiveMessage(ctx, ScanToBytes(t, s), mm)
			res := <-subject.SendChan

			if tt.shouldAck {
				res.Res <- messageResponse{err: nil}
			} else {
				res.Res <- messageResponse{err: errors.New("test-error")}
			}
			<-mm.Done
			assert.Equal(t, tt.shouldAck, mm.acked)
			assert.Equal(t, tt.shouldNack, mm.nacked)
			fn()
		})
	}
}

func TestReceiveMessageWillNackOnTimeout(t *testing.T) {
	ctx, fn := context.WithTimeout(context.Background(), time.Second*5)
	l := zaptest.NewLogger(t)

	subject := NewIngester(l, nil, WithMessageAckTimeout(time.Millisecond*100))

	s := newScan(1)
	mm := newMockMsg()
	// this method will block if no one is on the line, or if the channel
	// isn't buffered. So keep it in its own goroutine for tests
	go subject.receiveMessage(ctx, ScanToBytes(t, s), mm)
	<-subject.SendChan
	select {
	case <-time.After(time.Second * 1):
		assert.FailNow(t, "timer should not have gone off. Something failed with the timeout")
	case <-mm.Done:
		assert.Equal(t, false, mm.acked)
		assert.Equal(t, true, mm.nacked)
	}

	fn()
}

func newScan(version int) scanning.Scan {
	var dd interface{}
	if version == scanning.V1 {
		dd = &scanning.V1Data{
			ResponseBytesUtf8: []byte("service response: 222"),
		}
	} else if version == scanning.V2 {
		dd = &scanning.V2Data{
			ResponseStr: "service response: 111",
		}
	}

	s := scanning.Scan{
		Ip:          "1.1.1.1",
		Port:        53,
		Service:     "DNS",
		Timestamp:   time.Now().Unix(),
		DataVersion: version,
		Data:        dd,
	}
	return s
}

func newMockMsg() *mockMsg {
	return &mockMsg{
		Done: make(chan struct{}),
	}
}

type mockMsg struct {
	acked  bool
	nacked bool
	Done   chan struct{} // to signal to the test to check the result of the ack
}

func (m *mockMsg) Ack() {
	m.acked = true
	m.Done <- struct{}{}
}

func (m *mockMsg) Nack() {
	m.nacked = true
	m.Done <- struct{}{}
}

func ScanToBytes(t *testing.T, s scanning.Scan) []byte {
	encoded, err := json.Marshal(s)
	if err != nil {
		t.Fatal("failed to marshal scan to json bytes", err)
	}
	return encoded
}
