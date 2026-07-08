package feat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	minPollInterval  = 5 * time.Second
	maxDatafileBytes = 10 * 1024 * 1024
	defaultURL       = "https://data-01.feat.so"
	// defaultSafetyNetPollInterval is the slow cadence the background poll
	// runs at while the live stream is healthy. The stream is the live path;
	// this poll only exists to recover from a silently-wedged connection, so
	// it deliberately runs rarely to avoid needless fetches. When the stream
	// is down the poll reverts to PollInterval and becomes the primary refresh
	// path. Kept internal (not a Config field) to match the js-sdk.
	defaultSafetyNetPollInterval = 10 * time.Minute
)

// Config configures a Client. Only APIKey is required.
type Config struct {
	APIKey string
	// URL is the feat endpoint. Optional; defaults to the production
	// endpoint. Override for region pinning, staging, or local dev.
	URL string
	// PollInterval is the background refresh cadence. Defaults to 30s,
	// floored at 5s.
	PollInterval time.Duration
	// HTTPClient lets callers swap in a custom transport (e.g. for
	// fakes in tests). Defaults to http.DefaultClient.
	HTTPClient *http.Client
	// DisableStreaming turns off the live datafile stream. Streaming is on
	// by default: the client holds a Server-Sent Events connection and
	// adopts each pushed datafile the instant it changes, while the
	// background poll keeps running as a slow safety net. Set true to rely
	// on polling alone.
	DisableStreaming bool
	// OnStreamError, when set, receives errors from the live stream loop:
	// connection failures, non-recoverable HTTP statuses (such as a revoked
	// key or a forbidden origin), retryable statuses, and decode problems.
	// It is invoked on the stream goroutine, so keep it quick and
	// non-blocking. The poll loop is independent and keeps the datafile fresh
	// regardless, so a stream error is observability, not an outage.
	OnStreamError func(error)
}

// Client holds the in-memory datafile and refreshes it on a background
// interval. Cheap to call concurrently — evaluate is lock-free against
// the current datafile via atomic pointer load.
type Client struct {
	config     Config
	httpClient *http.Client
	datafile   atomic.Pointer[Datafile]
	etag       atomic.Pointer[string]
	stopCh     chan struct{}
	stopOnce   sync.Once
	startOnce  sync.Once
	// writeMu serializes datafile writers (the poll loop and the stream
	// loop) so the version-ordered "adopt only if newer" check is atomic.
	// Reads stay lock-free via the atomic pointer above.
	writeMu sync.Mutex
	// streamBackoffMin / streamBackoffMax bound the exponential reconnect
	// delay for the stream loop. Set in NewClient; overridable in tests.
	streamBackoffMin time.Duration
	streamBackoffMax time.Duration
	// safetyNetPollInterval is the slow cadence the poll runs at while the
	// stream is healthy. Computed in NewClient as max(defaultSafetyNet,
	// PollInterval) so the safety net is never faster than the configured
	// poll.
	safetyNetPollInterval time.Duration
	// streamConnected tracks live-stream health. It flips the poll cadence:
	// slow while the stream is up, PollInterval while it is down or disabled.
	streamConnected atomic.Bool
	// pollWake nudges the poll loop to reschedule its next fetch the instant
	// stream health changes, so a dropped stream falls back to the fast
	// interval right away rather than waiting out the remaining safety-net
	// delay. Buffered (size 1) and sent non-blocking: the loop always re-reads
	// the current state on wake, so one pending nudge is enough.
	pollWake chan struct{}
}

// NewClient returns a Client. Call Start to begin polling and Ready to
// wait for the first datafile.
func NewClient(cfg Config) (*Client, error) {
	if cfg.APIKey == "" {
		return nil, errors.New("feat: APIKey is required")
	}
	if cfg.URL == "" {
		cfg.URL = defaultURL
	}
	if err := assertHTTPS(cfg.URL); err != nil {
		return nil, err
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 30 * time.Second
	}
	if cfg.PollInterval < minPollInterval {
		cfg.PollInterval = minPollInterval
	}
	httpc := cfg.HTTPClient
	if httpc == nil {
		httpc = http.DefaultClient
	}
	// The safety net is never faster than the configured poll interval.
	safetyNet := defaultSafetyNetPollInterval
	if cfg.PollInterval > safetyNet {
		safetyNet = cfg.PollInterval
	}
	return &Client{
		config:                cfg,
		httpClient:            httpc,
		stopCh:                make(chan struct{}),
		streamBackoffMin:      defaultStreamBackoffMin,
		streamBackoffMax:      defaultStreamBackoffMax,
		safetyNetPollInterval: safetyNet,
		pollWake:              make(chan struct{}, 1),
	}, nil
}

// assertHTTPS rejects non-https URL so a misconfigured caller can't
// send the bearer token over plaintext. http://localhost and
// http://127.0.0.1 are allowed for local development and tests.
func assertHTTPS(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return errors.New("feat: URL is not a valid URL")
	}
	if u.Scheme == "https" {
		return nil
	}
	if u.Scheme == "http" && (u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1") {
		return nil
	}
	return errors.New("feat: URL must use https:// (http://localhost allowed for tests)")
}

// Start begins background refresh. Idempotent: calling twice is a no-op.
//
// The poll loop always runs as a safety net on a two-tier cadence: slow (10
// minutes) while the live stream is healthy, and reverting to the fast
// PollInterval the instant the stream drops or is disabled. Unless streaming
// is disabled, a second goroutine holds a live Server-Sent Events connection
// and adopts pushed datafiles in real time; if that stream drops it
// reconnects with backoff while the poll loop keeps the datafile fresh.
func (c *Client) Start(ctx context.Context) {
	c.startOnce.Do(func() {
		go c.pollLoop(ctx)
		if !c.config.DisableStreaming {
			go c.streamLoop(ctx)
		}
	})
}

// Ready blocks until the first datafile is in memory or ctx is cancelled.
// Returns a non-nil error if the first fetch fails.
func (c *Client) Ready(ctx context.Context) error {
	if c.datafile.Load() != nil {
		return nil
	}
	// One synchronous fetch — gives a fast failure mode on bad config.
	return c.fetchOnce(ctx)
}

// Close stops the background poller. Subsequent calls are no-ops.
func (c *Client) Close() {
	c.stopOnce.Do(func() { close(c.stopCh) })
}

// Refresh forces an immediate fetch. Useful for tests; not required in
// normal operation (the background poller handles it).
func (c *Client) Refresh(ctx context.Context) error {
	return c.fetchOnce(ctx)
}

func (c *Client) pollLoop(ctx context.Context) {
	if err := c.fetchOnce(ctx); err != nil {
		// Initial fetch failure: log and keep polling. The next tick may
		// succeed (transient network).
		fmt.Fprintf(io.Discard, "feat: initial fetch failed: %v\n", err)
	}
	// A single-shot timer we reschedule each cycle, rather than a fixed
	// ticker: the interval is chosen per cycle from the current stream health
	// (slow while streaming, PollInterval otherwise). pollWake lets the stream
	// loop reschedule us the instant health flips.
	timer := time.NewTimer(c.pollInterval())
	defer timer.Stop()
	for {
		select {
		case <-c.stopCh:
			return
		case <-ctx.Done():
			return
		case <-c.pollWake:
			// Stream health changed: reschedule the next poll with the new
			// cadence, measured from now. A dropped stream therefore falls back
			// to the fast interval promptly instead of waiting out the remaining
			// safety-net delay.
			resetTimer(timer, c.pollInterval())
		case <-timer.C:
			_ = c.fetchOnce(ctx)
			timer.Reset(c.pollInterval())
		}
	}
}

// pollInterval is the cadence for the next poll: the slow safety-net interval
// while the live stream is healthy, PollInterval when the stream is down or
// disabled (the poll is then the primary refresh path).
func (c *Client) pollInterval() time.Duration {
	if !c.config.DisableStreaming && c.streamConnected.Load() {
		return c.safetyNetPollInterval
	}
	return c.config.PollInterval
}

// setStreamConnected records stream health and, on a change, nudges the poll
// loop to reschedule with the new cadence. The nudge is non-blocking: if one
// is already pending the loop will re-read the current state when it wakes, so
// a dropped nudge cannot leave the cadence stale.
func (c *Client) setStreamConnected(connected bool) {
	if c.streamConnected.Swap(connected) == connected {
		return // no change
	}
	select {
	case c.pollWake <- struct{}{}:
	default:
	}
}

// resetTimer stops and drains t before rescheduling it for d, so a value left
// in the channel from a fire that raced the stop cannot trigger a spurious
// early poll.
func resetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}

func (c *Client) fetchOnce(ctx context.Context) error {
	url := strings.TrimSuffix(c.config.URL, "/") + "/sdk/v1/datafile"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.config.APIKey)
	req.Header.Set("User-Agent", "feat-sdk-go/"+Version)
	if etag := c.etag.Load(); etag != nil {
		req.Header.Set("If-None-Match", *etag)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusNotModified:
		return nil
	case http.StatusNotFound:
		// No datafile yet; treat as transient.
		return nil
	case http.StatusTooManyRequests:
		return nil
	case http.StatusOK:
		if resp.ContentLength > maxDatafileBytes {
			return errors.New("feat: datafile exceeds maximum allowed size")
		}
		var df Datafile
		limited := io.LimitReader(resp.Body, maxDatafileBytes+1)
		body, err := io.ReadAll(limited)
		if err != nil {
			return fmt.Errorf("feat: read datafile: %w", err)
		}
		if int64(len(body)) > maxDatafileBytes {
			return errors.New("feat: datafile exceeds maximum allowed size")
		}
		if err := json.Unmarshal(body, &df); err != nil {
			return fmt.Errorf("feat: decode datafile: %w", err)
		}
		c.adopt(&df)
		if e := resp.Header.Get("ETag"); e != "" {
			c.etag.Store(&e)
		}
		return nil
	default:
		return fmt.Errorf("feat: fetch datafile: %d", resp.StatusCode)
	}
}

// adopt stores df only if it is strictly newer than the datafile currently
// in memory (by Version), so an out-of-order poll response or a replayed
// stream frame can never roll the client back. The first datafile (current
// is nil) is always adopted. Returns true when df was stored. Writers are
// serialized by writeMu; readers continue to load the atomic pointer
// lock-free.
func (c *Client) adopt(df *Datafile) bool {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.adoptLocked(df)
}

// adoptLocked is the version-ordered store shared by every writer. The caller
// must already hold writeMu. The incremental patch path holds writeMu across
// its read-merge-store sequence and reuses this core so a patch and a
// concurrent poll/put can never interleave into a rollback.
func (c *Client) adoptLocked(df *Datafile) bool {
	if cur := c.datafile.Load(); cur != nil && df.Version <= cur.Version {
		return false
	}
	c.datafile.Store(df)
	return true
}

// Evaluate returns the raw evaluation result. Most callers want the typed
// helpers below.
func (c *Client) Evaluate(flagKey string, defaultValue json.RawMessage, ctx EvalContext) EvaluationResult {
	df := c.datafile.Load()
	if df == nil {
		return EvaluationResult{
			Value:        defaultValue,
			Reason:       ReasonError,
			ErrorMessage: "client not ready: call Ready() before Evaluate",
		}
	}
	return Evaluate(flagKey, defaultValue, ctx, df)
}

// GetBooleanValue evaluates a boolean flag. Returns defaultValue on
// type-mismatch or evaluation error.
func (c *Client) GetBooleanValue(flagKey string, defaultValue bool, ctx EvalContext) bool {
	r := c.Evaluate(flagKey, mustJSON(defaultValue), ctx)
	var v bool
	if err := json.Unmarshal(r.Value, &v); err != nil {
		return defaultValue
	}
	return v
}

func (c *Client) GetStringValue(flagKey, defaultValue string, ctx EvalContext) string {
	r := c.Evaluate(flagKey, mustJSON(defaultValue), ctx)
	var v string
	if err := json.Unmarshal(r.Value, &v); err != nil {
		return defaultValue
	}
	return v
}

func (c *Client) GetNumberValue(flagKey string, defaultValue float64, ctx EvalContext) float64 {
	r := c.Evaluate(flagKey, mustJSON(defaultValue), ctx)
	var v float64
	if err := json.Unmarshal(r.Value, &v); err != nil {
		return defaultValue
	}
	return v
}

// GetObjectValue unmarshals the JSON variation value into the supplied
// out pointer. Returns the same error JSON decoding would; on failure,
// out is left untouched and the caller should use its own default.
func (c *Client) GetObjectValue(flagKey string, ctx EvalContext, out any) error {
	r := c.Evaluate(flagKey, nil, ctx)
	if r.Value == nil {
		return errors.New(r.ErrorMessage)
	}
	return json.Unmarshal(r.Value, out)
}

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		return json.RawMessage("null")
	}
	return b
}
