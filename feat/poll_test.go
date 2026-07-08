package feat

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// While the stream is healthy the poll must sit on its slow safety-net cadence
// rather than the fast PollInterval: the stream is the live path, so the poll
// only needs to catch a silently-wedged connection.
func TestPollUsesSafetyNetCadenceWhileStreaming(t *testing.T) {
	srv := newSSEServer()
	srv.pollDF.Store(streamDatafile(1, false))
	ts := httptest.NewServer(srv.handler())
	defer ts.Close()

	c, err := NewClient(Config{APIKey: "feat_sdk_test", URL: ts.URL})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	c.streamBackoffMin = 5 * time.Millisecond
	c.streamBackoffMax = 20 * time.Millisecond
	// Set the intervals directly to bypass the 5s minPollInterval floor that
	// NewClient applies to the public Config.
	c.config.PollInterval = 20 * time.Millisecond // the fast fallback cadence
	c.safetyNetPollInterval = 2 * time.Second     // the slow, streaming-healthy cadence

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	defer c.Close()

	// Push a datafile so the stream connects and stays live.
	srv.push(streamDatafile(1, false))
	waitFor(t, 2*time.Second, func() bool { return c.streamConnected.Load() })

	// Let the reschedule to the slow cadence settle and drain any fast poll
	// that was already in flight when the stream came up.
	time.Sleep(40 * time.Millisecond)

	// Over a window several fast-intervals long the poll must stay on the 2s
	// safety-net cadence: at most a stray in-flight poll, far fewer than the
	// ~12 a 20ms cadence would produce.
	base := srv.pollHits.Load()
	time.Sleep(250 * time.Millisecond)
	if delta := srv.pollHits.Load() - base; delta > 1 {
		t.Fatalf("stream healthy: expected ~0 polls at the safety-net cadence, got %d in 250ms", delta)
	}
}

// When the stream drops, the poll must revert to the fast PollInterval right
// away and fetch promptly - not wait out the remaining safety-net delay.
func TestPollRevertsToFastCadenceWhenStreamDrops(t *testing.T) {
	srv := newSSEServer()
	srv.pollDF.Store(streamDatafile(1, false))
	ts := httptest.NewServer(srv.handler())
	defer ts.Close()

	c, err := NewClient(Config{APIKey: "feat_sdk_test", URL: ts.URL})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	c.streamBackoffMin = 5 * time.Millisecond
	c.streamBackoffMax = 20 * time.Millisecond
	// Set directly to bypass the 5s minPollInterval floor NewClient applies.
	c.config.PollInterval = 25 * time.Millisecond
	// Deliberately long: if the fast fallback is broken the test waits this out
	// and fails, rather than passing on the slow cadence.
	c.safetyNetPollInterval = 5 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	defer c.Close()

	srv.push(streamDatafile(1, false))
	waitFor(t, 2*time.Second, func() bool { return c.streamConnected.Load() })

	// Make future reconnects fail so the drop leaves the stream down (and the
	// poll as the primary path) instead of immediately reconnecting live again.
	srv.streamErr.Store(http.StatusInternalServerError)
	srv.dropConnection()
	waitFor(t, 2*time.Second, func() bool { return !c.streamConnected.Load() })

	// The next poll must arrive at the fast interval (~25ms). A generous 1s
	// bound still proves it did not wait out the 5s safety-net delay.
	base := srv.pollHits.Load()
	waitFor(t, 1*time.Second, func() bool { return srv.pollHits.Load() > base })
}

// With streaming disabled the poll is always the primary path and runs at the
// normal fast interval, never the (unused) safety-net cadence.
func TestPollUsesNormalCadenceWhenStreamingDisabled(t *testing.T) {
	srv := newSSEServer()
	srv.pollDF.Store(streamDatafile(1, false))
	ts := httptest.NewServer(srv.handler())
	defer ts.Close()

	c, err := NewClient(Config{
		APIKey:           "feat_sdk_test",
		URL:              ts.URL,
		DisableStreaming: true,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	// Set directly to bypass the 5s minPollInterval floor NewClient applies.
	c.config.PollInterval = 25 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	defer c.Close()

	if err := c.Ready(ctx); err != nil {
		t.Fatalf("Ready: %v", err)
	}

	// Over 250ms at a 25ms cadence we expect ~10 polls; assert a generous lower
	// bound so the test is not flaky under scheduler jitter.
	base := srv.pollHits.Load()
	time.Sleep(250 * time.Millisecond)
	if delta := srv.pollHits.Load() - base; delta < 3 {
		t.Fatalf("streaming disabled: expected multiple polls at the normal cadence, got %d in 250ms", delta)
	}
}
