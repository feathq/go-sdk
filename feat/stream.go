package feat

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	streamPath              = "/sdk/v1/datafile/stream"
	defaultStreamBackoffMin = 1 * time.Second
	defaultStreamBackoffMax = 30 * time.Second
)

// streamLoop holds a Server-Sent Events connection to the datafile stream
// endpoint and adopts every newer datafile the server pushes. It reconnects
// with exponential backoff and returns cleanly on Close or context
// cancellation. The background poll loop runs alongside it as a safety net,
// so a wedged or unreachable stream never leaves the datafile stale.
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

		// A connection that delivered at least one frame is healthy; reset
		// the backoff so a single long-lived stream that drops reconnects
		// promptly rather than inheriting a stale, grown delay.
		if gotData := c.stream(ctx); gotData {
			backoff = c.streamBackoffMin
		}

		select {
		case <-c.stopCh:
			return
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > c.streamBackoffMax {
			backoff = c.streamBackoffMax
		}
	}
}

// stream opens one SSE connection and reads frames until the connection ends
// or the client shuts down. It reports whether any frame was received so the
// caller can reset its reconnect backoff.
func (c *Client) stream(ctx context.Context) bool {
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
		return false
	}
	req.Header.Set("Authorization", "Bearer "+c.config.APIKey)
	req.Header.Set("User-Agent", "feat-sdk-go/"+Version)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	return c.readEvents(resp.Body)
}

// readEvents parses the SSE byte stream: it accumulates `event:`/`data:`/`id:`
// fields and dispatches a frame on each blank-line boundary. Lines beginning
// with ':' are comments (heartbeats) and are ignored. Only `put` frames carry
// a datafile. Returns true if any frame was parsed (the connection is live).
func (c *Client) readEvents(body io.Reader) bool {
	reader := bufio.NewReader(body)
	var (
		event   string
		data    strings.Builder
		gotData bool
	)
	dispatch := func() {
		defer func() {
			event = ""
			data.Reset()
		}()
		if event != "put" || data.Len() == 0 {
			return
		}
		if c.applyPut([]byte(data.String())) == nil {
			gotData = true
		}
	}
	for {
		line, err := reader.ReadString('\n')
		if len(line) > 0 {
			line = strings.TrimRight(line, "\r\n")
			switch {
			case line == "":
				dispatch()
			case strings.HasPrefix(line, ":"):
				// Heartbeat / comment line - ignore.
			default:
				field, value, _ := strings.Cut(line, ":")
				value = strings.TrimPrefix(value, " ")
				switch field {
				case "event":
					event = value
				case "data":
					// Bound memory: drop a frame whose data outgrows the max
					// datafile size rather than buffering it unbounded.
					if data.Len() <= maxDatafileBytes {
						if data.Len() > 0 {
							data.WriteByte('\n')
						}
						data.WriteString(value)
					}
				case "id":
					// The datafile version is carried inside the JSON payload;
					// the SSE id is informational and not used for ordering.
				}
			}
		}
		if err != nil {
			return gotData
		}
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
