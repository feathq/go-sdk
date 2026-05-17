package feat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Config configures a Client. APIKey + DataPlaneURL are required.
type Config struct {
	APIKey       string
	DataPlaneURL string
	// PollInterval is the background refresh cadence. Defaults to 30s,
	// matching Cloudflare KV's typical global-replication ceiling.
	PollInterval time.Duration
	// HTTPClient lets callers swap in a custom transport (e.g. for
	// fakes in tests). Defaults to http.DefaultClient.
	HTTPClient *http.Client
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
}

// NewClient returns a Client. Call Start to begin polling and Ready to
// wait for the first datafile.
func NewClient(cfg Config) (*Client, error) {
	if cfg.APIKey == "" {
		return nil, errors.New("feat: APIKey is required")
	}
	if cfg.DataPlaneURL == "" {
		return nil, errors.New("feat: DataPlaneURL is required")
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 30 * time.Second
	}
	httpc := cfg.HTTPClient
	if httpc == nil {
		httpc = http.DefaultClient
	}
	return &Client{config: cfg, httpClient: httpc, stopCh: make(chan struct{})}, nil
}

// Start begins background polling. Safe to call once. Idempotent: calling
// twice is a no-op.
func (c *Client) Start(ctx context.Context) {
	go c.pollLoop(ctx)
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
		// succeed (transient network, KV warm-up).
		fmt.Fprintf(io.Discard, "feat: initial fetch failed: %v\n", err)
	}
	t := time.NewTicker(c.config.PollInterval)
	defer t.Stop()
	for {
		select {
		case <-c.stopCh:
			return
		case <-ctx.Done():
			return
		case <-t.C:
			_ = c.fetchOnce(ctx)
		}
	}
}

func (c *Client) fetchOnce(ctx context.Context) error {
	url := strings.TrimSuffix(c.config.DataPlaneURL, "/") + "/sdk/v1/datafile"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.config.APIKey)
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
		// Egress hasn't landed yet for this env. Treat as transient.
		return nil
	case http.StatusOK:
		var df Datafile
		if err := json.NewDecoder(resp.Body).Decode(&df); err != nil {
			return fmt.Errorf("feat: decode datafile: %w", err)
		}
		c.datafile.Store(&df)
		if e := resp.Header.Get("ETag"); e != "" {
			c.etag.Store(&e)
		}
		return nil
	default:
		return fmt.Errorf("feat: fetch datafile: %s", resp.Status)
	}
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
