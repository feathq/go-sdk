package feat

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"time"
)

const (
	streamPath              = "/sdk/v1/datafile/stream"
	defaultStreamBackoffMin = 1 * time.Second
	defaultStreamBackoffMax = 30 * time.Second
)

// streamResult reports the outcome of a single stream connection so the
// reconnect loop can decide whether to reset its backoff and whether to keep
// trying at all.
type streamResult struct {
	// connected is true when the connection delivered at least one line
	// (a frame or a heartbeat), proving it was live.
	connected bool
	// terminal is true when the server rejected us in a way reconnecting
	// cannot fix (revoked/expired key, forbidden origin).
	terminal bool
}

// streamLoop holds a Server-Sent Events connection to the datafile stream
// endpoint and adopts every newer datafile the server pushes. It reconnects
// with jittered exponential backoff and returns cleanly on Close, context
// cancellation, or a terminal rejection. The background poll loop runs
// alongside it as a safety net, so a wedged, unreachable, or terminally
// rejected stream never leaves the datafile stale.
func (c *Client) streamLoop(ctx context.Context) {
	backoff := c.streamBackoffMin
	for {
		select {
		case <-c.stopCh:
			return
		case <-ctx.Done():
			return
		default:
		}

		res := c.stream(ctx)
		if res.connected {
			// A live connection (frame or heartbeat) resets the backoff so a
			// long-lived stream that later drops reconnects promptly rather
			// than inheriting a stale, grown delay.
			backoff = c.streamBackoffMin
		}
		if res.terminal {
			// The key is invalid/revoked/expired or the origin is forbidden.
			// Reconnecting every reconnect interval against a rejection it
			// cannot recover from is pure waste; stop and let the poll loop
			// stay the safety net.
			return
		}

		// Jittered wait in [backoff/2, backoff]. The jitter desynchronizes many
		// clients that lost the stream at the same instant (an endpoint blip)
		// so they do not reconnect in a synchronized storm. The un-jittered
		// backoff is what gets doubled for the next attempt.
		select {
		case <-c.stopCh:
			return
		case <-ctx.Done():
			return
		case <-time.After(jitterStreamBackoff(backoff)):
		}
		backoff = capStreamBackoff(backoff*2, c.streamBackoffMax)
	}
}

// jitterStreamBackoff returns a randomized delay in [backoff/2, backoff].
func jitterStreamBackoff(backoff time.Duration) time.Duration {
	if backoff <= 0 {
		return 0
	}
	return backoff/2 + time.Duration(rand.Int63n(int64(backoff/2)+1))
}

// capStreamBackoff clamps backoff to max.
func capStreamBackoff(backoff, max time.Duration) time.Duration {
	if backoff > max {
		return max
	}
	return backoff
}

// stream opens one SSE connection and reads frames until the connection ends
// or the client shuts down. It classifies the response so the caller can both
// reset its reconnect backoff on a healthy connection and stop reconnecting on
// a terminal rejection. Stream errors are surfaced through Config.OnStreamError
// when set; the poll loop is never blocked by them.
func (c *Client) stream(ctx context.Context) streamResult {
	// Derive a context that is also cancelled by Close(), so an in-flight
	// blocking read on the response body unblocks on shutdown.
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-c.stopCh:
			cancel()
		case <-sctx.Done():
		}
	}()

	url := strings.TrimSuffix(c.config.URL, "/") + streamPath
	req, err := http.NewRequestWithContext(sctx, http.MethodGet, url, nil)
	if err != nil {
		c.reportStreamError(fmt.Errorf("feat: build stream request: %w", err))
		return streamResult{}
	}
	req.Header.Set("Authorization", "Bearer "+c.config.APIKey)
	req.Header.Set("User-Agent", "feat-sdk-go/"+Version)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		// A cancel from Close or context shutdown is expected, not an error
		// worth surfacing to the operator.
		if sctx.Err() == nil {
			c.reportStreamError(fmt.Errorf("feat: stream connect: %w", err))
		}
		return streamResult{}
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusOK:
		// The stream is live: flip the poll to its slow safety-net cadence.
		// When readEvents returns (the connection ended), flip it back so the
		// poll immediately resumes as the primary refresh path.
		c.setStreamConnected(true)
		defer c.setStreamConnected(false)
		return streamResult{connected: c.readEvents(resp.Body)}
	case http.StatusUnauthorized, http.StatusForbidden:
		// 401: the API key is invalid, revoked, or expired.
		// 403: the request origin is not allowed.
		// Neither is recoverable by reconnecting.
		c.reportStreamError(fmt.Errorf("feat: stream rejected (status %d), not reconnecting", resp.StatusCode))
		return streamResult{terminal: true}
	default:
		// 429 / 5xx / anything else is transient: keep retrying with backoff.
		c.reportStreamError(fmt.Errorf("feat: stream status %d, retrying", resp.StatusCode))
		return streamResult{}
	}
}

// readEvents parses the SSE byte stream: it accumulates `event:`/`data:`/`id:`
// fields and dispatches a frame on each blank-line boundary. Lines beginning
// with ':' are comments (heartbeats) and are ignored. A `put` frame carries a
// full datafile; a `patch` frame carries an incremental delta. The body is
// wrapped in an io.LimitReader so a single oversized
// frame cannot be buffered without bound: the server sends the whole datafile
// JSON on one `data:` line, so the cap must bound the read itself, not just the
// accumulated builder. A line truncated by the limit is dropped as an
// incomplete frame and the connection ends (a reconnect reseeds). Returns true
// if any line was received, which proves the connection was live.
func (c *Client) readEvents(body io.Reader) bool {
	reader := bufio.NewReader(io.LimitReader(body, maxDatafileBytes+1))
	var (
		event   string
		data    strings.Builder
		gotLine bool
	)
	dispatch := func() {
		defer func() {
			event = ""
			data.Reset()
		}()
		if data.Len() == 0 {
			return
		}
		switch event {
		case "put":
			// A full datafile snapshot: adopt it if it is newer.
			if err := c.applyPut([]byte(data.String())); err != nil {
				c.reportStreamError(err)
			}
		case "patch":
			// An incremental delta: apply it only if it lands exactly on the
			// version in memory, otherwise ignore (reconnect re-seeds a put).
			if err := c.applyPatch([]byte(data.String())); err != nil {
				c.reportStreamError(err)
			}
		}
	}
	process := func(line string) {
		switch {
		case line == "":
			dispatch()
		case strings.HasPrefix(line, ":"):
			// Heartbeat / comment line - ignore (but it still counts as
			// liveness via gotLine below).
		default:
			field, value, _ := strings.Cut(line, ":")
			value = strings.TrimPrefix(value, " ")
			switch field {
			case "event":
				event = value
			case "data":
				if data.Len() > 0 {
					data.WriteByte('\n')
				}
				data.WriteString(value)
			case "id":
				// The datafile version is carried inside the JSON payload;
				// the SSE id is informational and not used for ordering.
			}
		}
	}
	for {
		line, err := reader.ReadString('\n')
		if err == nil {
			// A complete, newline-terminated line.
			gotLine = true
			process(strings.TrimRight(line, "\r\n"))
			continue
		}
		// err != nil: the connection ended. Any bytes left in line are an
		// unterminated, partial line - a frame truncated by the limit or by a
		// dropped connection. Drop it rather than parsing a half frame.
		if len(line) > 0 {
			c.reportStreamError(fmt.Errorf("feat: stream frame truncated, dropping incomplete frame"))
		}
		return gotLine
	}
}

// applyPut decodes a pushed datafile and adopts it if it is newer than the
// one in memory. A malformed or oversized payload is rejected without
// disturbing the current datafile.
func (c *Client) applyPut(data []byte) error {
	if int64(len(data)) > maxDatafileBytes {
		return fmt.Errorf("feat: streamed datafile exceeds maximum allowed size")
	}
	var df Datafile
	if err := json.Unmarshal(data, &df); err != nil {
		return fmt.Errorf("feat: decode streamed datafile: %w", err)
	}
	c.adopt(&df)
	return nil
}

// applyPatch decodes an incremental delta and applies it atomically when the
// in-memory datafile is exactly at the patch's From version. The whole
// read-merge-store runs under writeMu - the same lock the put and poll paths
// take - so a patch can never interleave with another writer into a rollback
// or a torn merge. A version mismatch or a gap (current != From, or no datafile
// seeded yet) is ignored without error: the client stays correct because a
// reconnect re-seeds a full put and the safety poll re-fetches. A malformed or
// oversized payload is rejected without disturbing the current datafile.
func (c *Client) applyPatch(data []byte) error {
	if int64(len(data)) > maxDatafileBytes {
		return fmt.Errorf("feat: streamed patch exceeds maximum allowed size")
	}
	var p datafilePatch
	if err := json.Unmarshal(data, &p); err != nil {
		return fmt.Errorf("feat: decode streamed patch: %w", err)
	}
	if p.To <= p.From {
		// Wire invariant: a patch must advance the version. A degenerate or
		// replayed backwards delta (to <= from) is ignored so it can never roll
		// version, etag, or generatedAt backward.
		return nil
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	cur := c.datafile.Load()
	if cur == nil || cur.Version != p.From {
		// Mismatch or gap: ignore. Reconnect (a fresh put) and the safety poll
		// keep the client correct; applying a delta onto the wrong base would
		// corrupt the datafile.
		return nil
	}

	if c.adoptLocked(patchDatafile(cur, &p)) && p.Etag != "" {
		// Advance the conditional-poll etag so the safety poll 304s instead of
		// re-downloading a datafile the patch already brought us to. An empty
		// patch etag is never stored: it would send an empty If-None-Match and
		// force a full 200, so the current pointer is kept instead.
		e := p.Etag
		c.etag.Store(&e)
	}
	return nil
}

// reportStreamError forwards a stream-loop error to the optional
// Config.OnStreamError callback. It runs on the stream goroutine and never
// touches the poll loop, so observability never blocks datafile updates.
func (c *Client) reportStreamError(err error) {
	if c.config.OnStreamError != nil {
		c.config.OnStreamError(err)
	}
}
