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
	return &Client{config: cfg, httpClient: httpc, stopCh: make(chan struct{})}, nil
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
		// succeed (transient network).
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
		c.datafile.Store(&df)
		if e := resp.Header.Get("ETag"); e != "" {
			c.etag.Store(&e)
		}
		return nil
	default:
		return fmt.Errorf("feat: fetch datafile: %d", resp.StatusCode)
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
