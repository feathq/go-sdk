package feat

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// checkoutFlag builds the single boolean flag "checkout" whose fallthrough
// variation is `on`. Evaluating it therefore reflects the datafile version in
// memory.
func checkoutFlag(on bool) FlagSpec {
	flag := boolFlag()
	if on {
		flag.DefaultVariationID = ptr(trueVar.ID)
	}
	return flag
}

// streamDatafile builds a datafile carrying a single boolean flag "checkout"
// whose fallthrough variation is `on`. Evaluating the flag therefore reflects
// which datafile version is currently in memory.
func streamDatafile(version int64, on bool) *Datafile {
	df := makeDatafile(map[string]FlagSpec{"checkout": checkoutFlag(on)}, nil)
	df.Version = version
	return df
}

func sseFrame(version int64, on bool) string {
	b, _ := json.Marshal(streamDatafile(version, on))
	return "event: put\nid: " + strconv.FormatInt(version, 10) + "\ndata: " + string(b) + "\n\n"
}

// patchFrame builds an `event: patch` SSE frame carrying the given delta. The
// id line is the target version, mirroring the put frames.
func patchFrame(p datafilePatch) string {
	b, _ := json.Marshal(p)
	return "event: patch\nid: " + strconv.FormatInt(p.To, 10) + "\ndata: " + string(b) + "\n\n"
}

// flipCheckout is a patch that flips the "checkout" flag's fallthrough to `on`,
// advancing version from->to and stamping a fresh etag.
func flipCheckout(from, to int64, on bool) datafilePatch {
	return datafilePatch{
		From:        from,
		To:          to,
		Etag:        "etag-" + strconv.FormatInt(to, 10),
		GeneratedAt: "2026-05-17T00:00:01Z",
		Flags:       map[string]FlagSpec{"checkout": checkoutFlag(on)},
	}
}

func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", d)
}

// sseServer is a fake data-plane that serves the SSE stream and the polling
// datafile endpoint. Datafiles to push are fed through pushCh; a nil value is
// a sentinel that drops the current stream connection (to exercise reconnect).
type sseServer struct {
	pushCh    chan *Datafile
	rawCh     chan string  // arbitrary pre-built SSE frames (e.g. patch frames)
	streamErr atomic.Int32 // when non-zero, the stream endpoint replies with this status
	oversize  atomic.Bool  // when set, the next connection emits one oversized frame then drops
	active    atomic.Int32 // currently open stream connections
	connects  atomic.Int32 // total stream connections accepted

	mu          sync.Mutex
	lastAuth    string
	lastHeaders http.Header

	pollDF atomic.Pointer[Datafile]
}

func newSSEServer() *sseServer {
	return &sseServer{pushCh: make(chan *Datafile, 8), rawCh: make(chan string, 8)}
}

func (s *sseServer) push(df *Datafile)    { s.pushCh <- df }
func (s *sseServer) pushRaw(frame string) { s.rawCh <- frame }
func (s *sseServer) dropConnection()      { s.pushCh <- nil }

func (s *sseServer) authHeader() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastAuth
}

func (s *sseServer) header(key string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastHeaders == nil {
		return ""
	}
	return s.lastHeaders.Get(key)
}

func (s *sseServer) handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/sdk/v1/datafile/stream", func(w http.ResponseWriter, r *http.Request) {
		s.connects.Add(1)
		s.mu.Lock()
		s.lastAuth = r.Header.Get("Authorization")
		s.lastHeaders = r.Header.Clone()
		s.mu.Unlock()

		if code := s.streamErr.Load(); code != 0 {
			w.WriteHeader(int(code))
			return
		}

		fl, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "no flush", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		// A heartbeat comment up front: the client must ignore it.
		_, _ = w.Write([]byte(": keep-alive\n\n"))
		fl.Flush()

		s.active.Add(1)
		defer s.active.Add(-1)

		// One-shot oversized frame: a single data line larger than the cap.
		// The client must drop it (truncated) and reconnect, after which this
		// connection serves normally.
		if s.oversize.CompareAndSwap(true, false) {
			_, _ = w.Write([]byte("event: put\ndata: "))
			_, _ = w.Write(make([]byte, maxDatafileBytes+10))
			fl.Flush()
			return
		}

		for {
			select {
			case <-r.Context().Done():
				return
			case raw := <-s.rawCh:
				_, _ = w.Write([]byte(raw))
				fl.Flush()
			case df := <-s.pushCh:
				if df == nil {
					return // sentinel: drop the connection
				}
				b, _ := json.Marshal(df)
				frame := "event: put\nid: " + strconv.FormatInt(df.Version, 10) +
					"\ndata: " + string(b) + "\n\n"
				_, _ = w.Write([]byte(frame))
				fl.Flush()
			}
		}
	})

	mux.HandleFunc("/sdk/v1/datafile", func(w http.ResponseWriter, r *http.Request) {
		df := s.pollDF.Load()
		if df == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(df)
	})

	return mux
}

func newStreamClient(t *testing.T, url string) *Client {
	t.Helper()
	c, err := NewClient(Config{
		APIKey:       "feat_sdk_test",
		URL:          url,
		PollInterval: time.Minute, // keep the safety poll out of the way
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	// Fast reconnect for tests.
	c.streamBackoffMin = 5 * time.Millisecond
	c.streamBackoffMax = 20 * time.Millisecond
	return c
}

// A newer-version put updates the datafile and a later evaluation reflects it;
// the Authorization header is sent on the stream request.
func TestStreamAdoptsNewerDatafile(t *testing.T) {
	srv := newSSEServer()
	ts := httptest.NewServer(srv.handler())
	defer ts.Close()

	c := newStreamClient(t, ts.URL)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	defer c.Close()

	srv.push(streamDatafile(1, false))
	waitFor(t, 2*time.Second, func() bool {
		df := c.datafile.Load()
		return df != nil && df.Version == 1
	})
	if got := c.GetBooleanValue("checkout", true, ctxUser("u1", nil)); got != false {
		t.Fatalf("version 1 should evaluate false, got %v", got)
	}

	srv.push(streamDatafile(2, true))
	waitFor(t, 2*time.Second, func() bool {
		return c.GetBooleanValue("checkout", false, ctxUser("u1", nil)) == true
	})
	if v := c.datafile.Load().Version; v != 2 {
		t.Fatalf("expected version 2, got %d", v)
	}

	if auth := srv.authHeader(); auth != "Bearer feat_sdk_test" {
		t.Fatalf("missing/incorrect Authorization header: %q", auth)
	}
}

// Equal and older versions pushed after a newer one are ignored.
func TestAdoptVersionOrdering(t *testing.T) {
	c, err := NewClient(Config{APIKey: "feat_sdk_test", URL: "https://example.test"})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if !c.adopt(streamDatafile(2, true)) {
		t.Fatal("first datafile should be adopted")
	}
	if c.adopt(streamDatafile(2, false)) {
		t.Fatal("equal version must be ignored")
	}
	if c.adopt(streamDatafile(1, false)) {
		t.Fatal("older version must be ignored")
	}
	if c.GetBooleanValue("checkout", false, ctxUser("u1", nil)) != true {
		t.Fatal("value must still reflect the adopted version 2 (true)")
	}
	if !c.adopt(streamDatafile(3, false)) {
		t.Fatal("newer version should be adopted")
	}
	if v := c.datafile.Load().Version; v != 3 {
		t.Fatalf("expected version 3, got %d", v)
	}
}

// The SSE parser handles heartbeats, applies a put, and ignores an out-of-order
// older frame that arrives after a newer one.
func TestReadEventsParsesAndOrders(t *testing.T) {
	c, err := NewClient(Config{APIKey: "feat_sdk_test", URL: "https://example.test"})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	body := strings.NewReader(
		": ping\n\n" + // heartbeat
			sseFrame(2, true) +
			sseFrame(1, false) + // older: ignored
			sseFrame(3, false), // newer: adopted
	)
	if got := c.readEvents(body); !got {
		t.Fatal("readEvents should report it received data")
	}
	df := c.datafile.Load()
	if df == nil || df.Version != 3 {
		t.Fatalf("expected version 3 in memory, got %+v", df)
	}
	if c.GetBooleanValue("checkout", true, ctxUser("u1", nil)) != false {
		t.Fatal("value should reflect version 3 (false)")
	}
}

// When the stream drops, the client reconnects with backoff and resumes
// receiving pushes.
func TestStreamReconnects(t *testing.T) {
	srv := newSSEServer()
	ts := httptest.NewServer(srv.handler())
	defer ts.Close()

	c := newStreamClient(t, ts.URL)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	defer c.Close()

	srv.push(streamDatafile(1, false))
	waitFor(t, 2*time.Second, func() bool {
		df := c.datafile.Load()
		return df != nil && df.Version == 1
	})

	srv.dropConnection() // server returns; client must reconnect
	srv.push(streamDatafile(2, true))
	waitFor(t, 2*time.Second, func() bool {
		return c.GetBooleanValue("checkout", false, ctxUser("u1", nil)) == true
	})
	if n := srv.connects.Load(); n < 2 {
		t.Fatalf("expected at least 2 stream connections after reconnect, got %d", n)
	}
}

// When the stream endpoint is unavailable, polling still populates the
// datafile - streaming is best-effort and the poll path is the safety net.
func TestPollFallbackWhenStreamFails(t *testing.T) {
	srv := newSSEServer()
	srv.streamErr.Store(http.StatusInternalServerError)
	srv.pollDF.Store(streamDatafile(7, true))
	ts := httptest.NewServer(srv.handler())
	defer ts.Close()

	c := newStreamClient(t, ts.URL)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	defer c.Close()

	// Ready performs a synchronous poll fetch, which succeeds even though the
	// stream endpoint is failing.
	if err := c.Ready(ctx); err != nil {
		t.Fatalf("Ready: %v", err)
	}
	if c.GetBooleanValue("checkout", false, ctxUser("u1", nil)) != true {
		t.Fatal("poll fallback should have loaded version 7 (true)")
	}
	if v := c.datafile.Load().Version; v != 7 {
		t.Fatalf("expected version 7 from poll, got %d", v)
	}
	// The stream was still attempted.
	waitFor(t, 2*time.Second, func() bool { return srv.connects.Load() >= 1 })
}

// Close tears down the stream goroutine and the open connection.
func TestStreamClosesCleanly(t *testing.T) {
	srv := newSSEServer()
	ts := httptest.NewServer(srv.handler())
	defer ts.Close()

	c := newStreamClient(t, ts.URL)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)

	srv.push(streamDatafile(1, false))
	waitFor(t, 2*time.Second, func() bool { return srv.active.Load() == 1 })

	c.Close()
	waitFor(t, 2*time.Second, func() bool { return srv.active.Load() == 0 })

	// Double Close is a no-op.
	c.Close()
}

// Cancelling the context tears down the stream goroutine and the connection.
func TestStreamStopsOnContextCancel(t *testing.T) {
	srv := newSSEServer()
	ts := httptest.NewServer(srv.handler())
	defer ts.Close()

	c := newStreamClient(t, ts.URL)
	ctx, cancel := context.WithCancel(context.Background())
	c.Start(ctx)
	defer c.Close()

	srv.push(streamDatafile(1, false))
	waitFor(t, 2*time.Second, func() bool { return srv.active.Load() == 1 })

	cancel()
	waitFor(t, 2*time.Second, func() bool { return srv.active.Load() == 0 })
}

// Disabling streaming starts no stream goroutine; polling alone drives updates.
func TestStreamingDisabledUsesPollOnly(t *testing.T) {
	srv := newSSEServer()
	srv.pollDF.Store(streamDatafile(4, true))
	ts := httptest.NewServer(srv.handler())
	defer ts.Close()

	c, err := NewClient(Config{
		APIKey:           "feat_sdk_test",
		URL:              ts.URL,
		PollInterval:     time.Minute,
		DisableStreaming: true,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	defer c.Close()

	if err := c.Ready(ctx); err != nil {
		t.Fatalf("Ready: %v", err)
	}
	if c.GetBooleanValue("checkout", false, ctxUser("u1", nil)) != true {
		t.Fatal("poll-only client should have loaded version 4 (true)")
	}
	// Give any (incorrectly spawned) stream goroutine a chance to connect.
	time.Sleep(50 * time.Millisecond)
	if n := srv.connects.Load(); n != 0 {
		t.Fatalf("streaming disabled but stream endpoint was hit %d times", n)
	}
}

// errSink collects errors handed to Config.OnStreamError from the stream
// goroutine; it is safe for concurrent use.
type errSink struct {
	mu   sync.Mutex
	errs []error
}

func (e *errSink) record(err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.errs = append(e.errs, err)
}

func (e *errSink) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.errs)
}

func bareClient(t *testing.T) *Client {
	t.Helper()
	c, err := NewClient(Config{APIKey: "feat_sdk_test", URL: "https://example.test"})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return c
}

// A malformed put leaves the in-memory datafile untouched and does not panic.
func TestApplyPutMalformedJSONLeavesDatafileIntact(t *testing.T) {
	c := bareClient(t)
	if !c.adopt(streamDatafile(5, true)) {
		t.Fatal("seed datafile should be adopted")
	}
	if err := c.applyPut([]byte("{ this is not valid json")); err == nil {
		t.Fatal("malformed put should return a decode error")
	}
	df := c.datafile.Load()
	if df == nil || df.Version != 5 {
		t.Fatalf("datafile must be undisturbed at version 5, got %+v", df)
	}
	if c.GetBooleanValue("checkout", false, ctxUser("u1", nil)) != true {
		t.Fatal("value must still reflect the seeded version 5 (true)")
	}
}

// An oversized put is rejected by applyPut without adopting and without
// disturbing the current datafile.
func TestApplyPutOversizedRejected(t *testing.T) {
	c := bareClient(t)
	if !c.adopt(streamDatafile(3, true)) {
		t.Fatal("seed datafile should be adopted")
	}
	if err := c.applyPut([]byte(strings.Repeat("a", maxDatafileBytes+1))); err == nil {
		t.Fatal("oversized put should be rejected")
	}
	if v := c.datafile.Load().Version; v != 3 {
		t.Fatalf("datafile must be undisturbed at version 3, got %d", v)
	}
}

// A data line larger than the cap is dropped (truncated) by the parser without
// adopting, while a valid frame received before it stays applied. The read is
// bounded, so the oversized line is never fully buffered.
func TestReadEventsDropsOversizedFrame(t *testing.T) {
	c := bareClient(t)
	big := strings.Repeat("a", maxDatafileBytes+10)
	body := strings.NewReader(sseFrame(5, true) + "event: put\ndata: " + big + "\n\n")
	c.readEvents(body)
	df := c.datafile.Load()
	if df == nil || df.Version != 5 {
		t.Fatalf("the valid frame before the oversized one must be adopted (v5), got %+v", df)
	}
}

// Over the wire, an oversized frame drops the connection but does not wedge the
// client: it reconnects and adopts the next valid frame.
func TestStreamRecoversAfterOversizedFrame(t *testing.T) {
	srv := newSSEServer()
	srv.oversize.Store(true)
	ts := httptest.NewServer(srv.handler())
	defer ts.Close()

	c := newStreamClient(t, ts.URL)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	defer c.Close()

	// First connection serves the oversized frame and drops; the client
	// reconnects, and this push lands on the healthy second connection.
	waitFor(t, 2*time.Second, func() bool { return srv.connects.Load() >= 2 })
	srv.push(streamDatafile(9, true))
	waitFor(t, 2*time.Second, func() bool {
		df := c.datafile.Load()
		return df != nil && df.Version == 9
	})
}

// 401 and 403 are terminal: the client surfaces the error and stops
// reconnecting rather than hammering a rejection it cannot recover from.
func TestStreamTerminalStatusStops(t *testing.T) {
	for _, code := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			srv := newSSEServer()
			srv.streamErr.Store(int32(code))
			ts := httptest.NewServer(srv.handler())
			defer ts.Close()

			sink := &errSink{}
			c, err := NewClient(Config{
				APIKey:        "feat_sdk_test",
				URL:           ts.URL,
				PollInterval:  time.Minute,
				OnStreamError: sink.record,
			})
			if err != nil {
				t.Fatalf("NewClient: %v", err)
			}
			c.streamBackoffMin = 5 * time.Millisecond
			c.streamBackoffMax = 20 * time.Millisecond

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c.Start(ctx)
			defer c.Close()

			waitFor(t, 2*time.Second, func() bool { return srv.connects.Load() >= 1 })
			// Ample time for the loop to (wrongly) reconnect if it were going to;
			// many backoff intervals would elapse here.
			time.Sleep(150 * time.Millisecond)
			if n := srv.connects.Load(); n != 1 {
				t.Fatalf("status %d is terminal: expected exactly 1 connect, got %d", code, n)
			}
			if sink.count() == 0 {
				t.Fatalf("status %d should surface a stream error to OnStreamError", code)
			}
		})
	}
}

// 429 is transient: the client keeps reconnecting (the poll loop also covers
// it, but the stream itself must keep trying).
func TestStreamRetriesOnRateLimit(t *testing.T) {
	srv := newSSEServer()
	srv.streamErr.Store(http.StatusTooManyRequests)
	ts := httptest.NewServer(srv.handler())
	defer ts.Close()

	c := newStreamClient(t, ts.URL)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	defer c.Close()

	waitFor(t, 2*time.Second, func() bool { return srv.connects.Load() >= 2 })
}

// Backoff grows on repeated failure and is capped at the max.
func TestBackoffGrowsAndCaps(t *testing.T) {
	min := 5 * time.Millisecond
	max := 40 * time.Millisecond
	want := []time.Duration{10, 20, 40, 40, 40}
	b := min
	for i, w := range want {
		b = capStreamBackoff(b*2, max)
		if b != w*time.Millisecond {
			t.Fatalf("step %d: backoff = %s, want %dms", i, b, w)
		}
	}
}

// Jitter keeps the delay within [backoff/2, backoff].
func TestJitterBounds(t *testing.T) {
	const backoff = 20 * time.Millisecond
	for i := 0; i < 2000; i++ {
		j := jitterStreamBackoff(backoff)
		if j < backoff/2 || j > backoff {
			t.Fatalf("jitter %s out of [%s, %s]", j, backoff/2, backoff)
		}
	}
	if got := jitterStreamBackoff(0); got != 0 {
		t.Fatalf("zero backoff should not panic and should return 0, got %s", got)
	}
}

// A connection carrying only heartbeats is live: readEvents reports it as such
// (so the reconnect loop resets its backoff) yet adopts no datafile.
func TestHeartbeatOnlyConnectionIsLive(t *testing.T) {
	c := bareClient(t)
	if !c.readEvents(strings.NewReader(": keep-alive\n\n: ping\n\n")) {
		t.Fatal("a heartbeat-only connection must report as live to reset backoff")
	}
	if c.datafile.Load() != nil {
		t.Fatal("a heartbeat carries no datafile; nothing should be adopted")
	}
}

// The stream request carries the SSE handshake headers.
func TestStreamSendsSSEHeaders(t *testing.T) {
	srv := newSSEServer()
	ts := httptest.NewServer(srv.handler())
	defer ts.Close()

	c := newStreamClient(t, ts.URL)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	defer c.Close()

	srv.push(streamDatafile(1, false))
	waitFor(t, 2*time.Second, func() bool { return srv.connects.Load() >= 1 })

	if got := srv.header("Accept"); got != "text/event-stream" {
		t.Fatalf("Accept header = %q, want text/event-stream", got)
	}
	if got := srv.header("Cache-Control"); got != "no-cache" {
		t.Fatalf("Cache-Control header = %q, want no-cache", got)
	}
}

// A patch whose From matches the in-memory version applies atomically: the
// changed flag is reflected by a later evaluation, and version + etag advance.
func TestApplyPatchAppliesWhenVersionMatches(t *testing.T) {
	c := bareClient(t)
	if !c.adopt(streamDatafile(1, false)) {
		t.Fatal("seed datafile should be adopted")
	}
	if c.GetBooleanValue("checkout", true, ctxUser("u1", nil)) != false {
		t.Fatal("seed (v1) should evaluate false")
	}

	if err := c.applyPatch(mustJSON(flipCheckout(1, 2, true))); err != nil {
		t.Fatalf("applyPatch: %v", err)
	}

	if c.GetBooleanValue("checkout", false, ctxUser("u1", nil)) != true {
		t.Fatal("after patch the checkout flag should evaluate true")
	}
	df := c.datafile.Load()
	if df.Version != 2 {
		t.Fatalf("version should advance to 2, got %d", df.Version)
	}
	if df.Etag != "etag-2" {
		t.Fatalf("datafile etag should advance to etag-2, got %q", df.Etag)
	}
	if e := c.etag.Load(); e == nil || *e != "etag-2" {
		t.Fatalf("conditional-poll etag should advance to etag-2, got %v", e)
	}
}

// A patch listing a key in removedFlags drops that flag; a later evaluation of
// it falls back to the caller default with an ERROR reason.
func TestApplyPatchRemovesFlag(t *testing.T) {
	c := bareClient(t)
	if !c.adopt(streamDatafile(1, true)) {
		t.Fatal("seed datafile should be adopted")
	}
	patch := datafilePatch{From: 1, To: 2, Etag: "etag-2", RemovedFlags: []string{"checkout"}}
	if err := c.applyPatch(mustJSON(patch)); err != nil {
		t.Fatalf("applyPatch: %v", err)
	}
	if _, ok := c.datafile.Load().Flags["checkout"]; ok {
		t.Fatal("removed flag must be gone from the datafile")
	}
	r := c.Evaluate("checkout", raw("fallback"), ctxUser("u1", nil))
	if r.Reason != ReasonError {
		t.Fatalf("evaluating a removed flag should be ERROR, got %v", r.Reason)
	}
}

// A patch merges added/changed segments and drops removed ones, and the new
// segment definitions take effect in evaluation immediately.
func TestApplyPatchMergesAndRemovesSegments(t *testing.T) {
	c := bareClient(t)
	// Seed: a flag gated on segment "internal-users", plus that segment.
	flag := boolFlag()
	flag.Rules = []RuleSpec{{
		ID:          "r1",
		VariationID: ptr(trueVar.ID),
		Groups: []ConditionGroupSpec{{
			Conditions: []ConditionSpec{{
				Operator: "segment_match",
				Values:   []json.RawMessage{raw("internal-users")},
			}},
		}},
	}}
	seg := SegmentSpec{Key: "internal-users", Rules: []SegmentRuleSpec{{
		Conditions: []ConditionSpec{{
			AttributePath: "user.email",
			Operator:      "ends_with",
			Values:        []json.RawMessage{raw("@feathq.com")},
		}},
	}}}
	df := makeDatafile(map[string]FlagSpec{"checkout": flag}, map[string]SegmentSpec{"internal-users": seg})
	df.Version = 1
	if !c.adopt(df) {
		t.Fatal("seed datafile should be adopted")
	}

	insider := ctxUser("u1", map[string]any{"email": "bob@feathq.com"})
	if c.GetBooleanValue("checkout", false, insider) != true {
		t.Fatal("seed: insider should match the segment and get true")
	}

	// Patch: narrow the segment to a different domain. The insider no longer
	// matches; a contractor does.
	narrowed := SegmentSpec{Key: "internal-users", Rules: []SegmentRuleSpec{{
		Conditions: []ConditionSpec{{
			AttributePath: "user.email",
			Operator:      "ends_with",
			Values:        []json.RawMessage{raw("@contractor.feathq.com")},
		}},
	}}}
	patch := datafilePatch{
		From:     1,
		To:       2,
		Etag:     "etag-2",
		Segments: map[string]SegmentSpec{"internal-users": narrowed},
	}
	if err := c.applyPatch(mustJSON(patch)); err != nil {
		t.Fatalf("applyPatch: %v", err)
	}
	if c.GetBooleanValue("checkout", true, insider) != false {
		t.Fatal("after the segment patch the insider should no longer match")
	}
	contractor := ctxUser("u2", map[string]any{"email": "x@contractor.feathq.com"})
	if c.GetBooleanValue("checkout", false, contractor) != true {
		t.Fatal("after the segment patch the contractor should match")
	}

	// A removedSegments patch drops the segment entirely: nobody matches.
	drop := datafilePatch{From: 2, To: 3, Etag: "etag-3", RemovedSegments: []string{"internal-users"}}
	if err := c.applyPatch(mustJSON(drop)); err != nil {
		t.Fatalf("applyPatch: %v", err)
	}
	if _, ok := c.datafile.Load().Segments["internal-users"]; ok {
		t.Fatal("removed segment must be gone from the datafile")
	}
	if c.GetBooleanValue("checkout", true, contractor) != false {
		t.Fatal("with the segment removed, the contractor should no longer match")
	}
}

// A patch whose From does not match the in-memory version is ignored: the
// datafile is left untouched (a reconnect re-seeds a full put).
func TestApplyPatchIgnoredOnVersionMismatch(t *testing.T) {
	c := bareClient(t)
	if !c.adopt(streamDatafile(2, false)) {
		t.Fatal("seed datafile should be adopted")
	}
	// from=1 but memory is at version 2: a gap.
	if err := c.applyPatch(mustJSON(flipCheckout(1, 3, true))); err != nil {
		t.Fatalf("a mismatched patch must be ignored without error, got %v", err)
	}
	df := c.datafile.Load()
	if df.Version != 2 {
		t.Fatalf("datafile must stay at version 2, got %d", df.Version)
	}
	if c.GetBooleanValue("checkout", true, ctxUser("u1", nil)) != false {
		t.Fatal("value must still reflect the un-patched version 2 (false)")
	}
}

// A patch arriving before any datafile is seeded is ignored without error.
func TestApplyPatchIgnoredWhenNotSeeded(t *testing.T) {
	c := bareClient(t)
	if err := c.applyPatch(mustJSON(flipCheckout(0, 1, true))); err != nil {
		t.Fatalf("a patch with no seeded datafile must be ignored, got %v", err)
	}
	if c.datafile.Load() != nil {
		t.Fatal("no datafile should be present")
	}
}

// A malformed patch payload returns a decode error and leaves the datafile
// untouched.
func TestApplyPatchMalformedJSONLeavesDatafileIntact(t *testing.T) {
	c := bareClient(t)
	if !c.adopt(streamDatafile(5, true)) {
		t.Fatal("seed datafile should be adopted")
	}
	if err := c.applyPatch([]byte("{ this is not valid json")); err == nil {
		t.Fatal("malformed patch should return a decode error")
	}
	df := c.datafile.Load()
	if df == nil || df.Version != 5 {
		t.Fatalf("datafile must be undisturbed at version 5, got %+v", df)
	}
	if c.GetBooleanValue("checkout", false, ctxUser("u1", nil)) != true {
		t.Fatal("value must still reflect the seeded version 5 (true)")
	}
}

// An oversized patch payload is rejected without disturbing the datafile.
func TestApplyPatchOversizedRejected(t *testing.T) {
	c := bareClient(t)
	if !c.adopt(streamDatafile(3, true)) {
		t.Fatal("seed datafile should be adopted")
	}
	if err := c.applyPatch([]byte(strings.Repeat("a", maxDatafileBytes+1))); err == nil {
		t.Fatal("oversized patch should be rejected")
	}
	if v := c.datafile.Load().Version; v != 3 {
		t.Fatalf("datafile must be undisturbed at version 3, got %d", v)
	}
}

// Chained patches each land on the version the previous one produced, walking
// the datafile forward one delta at a time.
func TestApplyPatchChained(t *testing.T) {
	c := bareClient(t)
	if !c.adopt(streamDatafile(1, false)) {
		t.Fatal("seed datafile should be adopted")
	}
	for _, p := range []datafilePatch{
		flipCheckout(1, 2, true),
		flipCheckout(2, 3, false),
		flipCheckout(3, 4, true),
	} {
		if err := c.applyPatch(mustJSON(p)); err != nil {
			t.Fatalf("applyPatch %d->%d: %v", p.From, p.To, err)
		}
	}
	df := c.datafile.Load()
	if df.Version != 4 {
		t.Fatalf("chained patches should reach version 4, got %d", df.Version)
	}
	if c.GetBooleanValue("checkout", false, ctxUser("u1", nil)) != true {
		t.Fatal("final value should reflect the last patch (true)")
	}
}

// The SSE parser applies a patch that follows a put on the same connection.
func TestReadEventsAppliesPatchAfterPut(t *testing.T) {
	c := bareClient(t)
	body := strings.NewReader(sseFrame(1, false) + patchFrame(flipCheckout(1, 2, true)))
	if got := c.readEvents(body); !got {
		t.Fatal("readEvents should report it received data")
	}
	df := c.datafile.Load()
	if df == nil || df.Version != 2 {
		t.Fatalf("expected version 2 after put+patch, got %+v", df)
	}
	if c.GetBooleanValue("checkout", false, ctxUser("u1", nil)) != true {
		t.Fatal("value should reflect the patched version 2 (true)")
	}
}

// Over the wire: a put seeds the datafile, then a patch frame on the same
// connection is applied and reflected by a later evaluation.
func TestStreamAppliesPatch(t *testing.T) {
	srv := newSSEServer()
	ts := httptest.NewServer(srv.handler())
	defer ts.Close()

	c := newStreamClient(t, ts.URL)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	defer c.Close()

	srv.push(streamDatafile(1, false))
	waitFor(t, 2*time.Second, func() bool {
		df := c.datafile.Load()
		return df != nil && df.Version == 1
	})

	srv.pushRaw(patchFrame(flipCheckout(1, 2, true)))
	waitFor(t, 2*time.Second, func() bool {
		return c.GetBooleanValue("checkout", false, ctxUser("u1", nil)) == true
	})
	if v := c.datafile.Load().Version; v != 2 {
		t.Fatalf("expected version 2 after patch, got %d", v)
	}
}

// Over the wire: a patch that does not land on the current version is ignored,
// leaving the datafile untouched.
func TestStreamPatchIgnoredOnGap(t *testing.T) {
	srv := newSSEServer()
	ts := httptest.NewServer(srv.handler())
	defer ts.Close()

	c := newStreamClient(t, ts.URL)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	defer c.Close()

	srv.push(streamDatafile(1, false))
	waitFor(t, 2*time.Second, func() bool {
		df := c.datafile.Load()
		return df != nil && df.Version == 1
	})

	// A patch from version 5 (a gap) must be ignored. Follow it with a valid
	// 1->2 patch so we can wait on an observable effect and prove the gapped
	// one did not slip through.
	srv.pushRaw(patchFrame(flipCheckout(5, 6, true)))
	srv.pushRaw(patchFrame(flipCheckout(1, 2, true)))
	waitFor(t, 2*time.Second, func() bool {
		return c.datafile.Load().Version == 2
	})
	if c.GetBooleanValue("checkout", false, ctxUser("u1", nil)) != true {
		t.Fatal("the valid 1->2 patch should have applied (true)")
	}
}

// A patch whose To does not advance past From (to <= from) is refused before it
// can apply, so a degenerate or replayed backwards delta never rolls version,
// etag, or value backward. Both the equal (to == from) and backward (to < from)
// cases are checked.
func TestApplyPatchRejectsNonAdvancing(t *testing.T) {
	for _, to := range []int64{2, 1} { // equal, then backward
		c := bareClient(t)
		if !c.adopt(streamDatafile(2, false)) {
			t.Fatal("seed datafile should be adopted")
		}
		// from=2 matches the in-memory version, so only the to<=from guard can
		// stop this from applying.
		if err := c.applyPatch(mustJSON(flipCheckout(2, to, true))); err != nil {
			t.Fatalf("to=%d: non-advancing patch must be ignored without error, got %v", to, err)
		}
		df := c.datafile.Load()
		if df.Version != 2 {
			t.Fatalf("to=%d: version must stay 2, got %d", to, df.Version)
		}
		if df.Etag != "etag" {
			t.Fatalf("to=%d: etag must stay %q, got %q", to, "etag", df.Etag)
		}
		if c.GetBooleanValue("checkout", true, ctxUser("u1", nil)) != false {
			t.Fatalf("to=%d: value must still reflect the un-patched v2 (false)", to)
		}
	}
}

// Applying a patch advances generatedAt to the patch value, not just version and
// etag.
func TestApplyPatchAdvancesGeneratedAt(t *testing.T) {
	c := bareClient(t)
	if !c.adopt(streamDatafile(1, false)) {
		t.Fatal("seed datafile should be adopted")
	}
	if got := c.datafile.Load().GeneratedAt; got != "2026-05-17T00:00:00Z" {
		t.Fatalf("seed generatedAt = %q, want the makeDatafile default", got)
	}
	if err := c.applyPatch(mustJSON(flipCheckout(1, 2, true))); err != nil {
		t.Fatalf("applyPatch: %v", err)
	}
	if got := c.datafile.Load().GeneratedAt; got != "2026-05-17T00:00:01Z" {
		t.Fatalf("generatedAt should advance to the patch value, got %q", got)
	}
}

// A patch that omits etag and generatedAt keeps the current metadata rather than
// wiping it to "": the datafile fields are preserved and the conditional-poll
// etag pointer is left intact so the safety poll still sends a real
// If-None-Match (an empty one would force a full 200).
func TestApplyPatchKeepsMetadataWhenOmitted(t *testing.T) {
	c := bareClient(t)
	if !c.adopt(streamDatafile(1, false)) {
		t.Fatal("seed datafile should be adopted")
	}
	prevEtag := "etag-seed"
	c.etag.Store(&prevEtag)

	// A patch that carries flags but neither etag nor generatedAt.
	p := datafilePatch{From: 1, To: 2, Flags: map[string]FlagSpec{"checkout": checkoutFlag(true)}}
	if err := c.applyPatch(mustJSON(p)); err != nil {
		t.Fatalf("applyPatch: %v", err)
	}
	df := c.datafile.Load()
	if df.Version != 2 {
		t.Fatalf("version should advance to 2, got %d", df.Version)
	}
	if df.Etag != "etag" {
		t.Fatalf("datafile etag should be preserved as %q, got %q", "etag", df.Etag)
	}
	if df.GeneratedAt != "2026-05-17T00:00:00Z" {
		t.Fatalf("datafile generatedAt should be preserved, got %q", df.GeneratedAt)
	}
	if e := c.etag.Load(); e == nil || *e != prevEtag {
		t.Fatalf("conditional-poll etag must not be wiped by an omitted patch etag, got %v", e)
	}
	if c.GetBooleanValue("checkout", false, ctxUser("u1", nil)) != true {
		t.Fatal("the patch flags should still apply even without metadata")
	}
}

// Applying the same from->to patch twice is idempotent: the second application
// lands on the already-advanced version (cur != from) and is ignored, leaving
// the state unchanged.
func TestApplyPatchIdempotent(t *testing.T) {
	c := bareClient(t)
	if !c.adopt(streamDatafile(1, false)) {
		t.Fatal("seed datafile should be adopted")
	}
	p := flipCheckout(1, 2, true)
	if err := c.applyPatch(mustJSON(p)); err != nil {
		t.Fatalf("first applyPatch: %v", err)
	}
	if err := c.applyPatch(mustJSON(p)); err != nil {
		t.Fatalf("replayed applyPatch must be ignored without error, got %v", err)
	}
	df := c.datafile.Load()
	if df.Version != 2 {
		t.Fatalf("version should stay 2 after the replayed patch, got %d", df.Version)
	}
	if c.GetBooleanValue("checkout", false, ctxUser("u1", nil)) != true {
		t.Fatal("value should still reflect the single application (true)")
	}
}

// A stale patch whose target the client is already past (cur >= to) is ignored:
// memory at v3, an old 1->2 delta lands on neither the current version nor a
// forward step.
func TestApplyPatchIgnoredWhenAlreadyPastTarget(t *testing.T) {
	c := bareClient(t)
	if !c.adopt(streamDatafile(3, false)) {
		t.Fatal("seed datafile should be adopted")
	}
	if err := c.applyPatch(mustJSON(flipCheckout(1, 2, true))); err != nil {
		t.Fatalf("a superseded patch must be ignored without error, got %v", err)
	}
	df := c.datafile.Load()
	if df.Version != 3 {
		t.Fatalf("version must stay 3, got %d", df.Version)
	}
	if c.GetBooleanValue("checkout", true, ctxUser("u1", nil)) != false {
		t.Fatal("value must still reflect the un-patched v3 (false)")
	}
}

// ContextKinds are not part of a patch and must survive it unchanged.
func TestApplyPatchPreservesContextKinds(t *testing.T) {
	c := bareClient(t)
	if !c.adopt(streamDatafile(1, false)) {
		t.Fatal("seed datafile should be adopted")
	}
	before := c.datafile.Load().ContextKinds
	if err := c.applyPatch(mustJSON(flipCheckout(1, 2, true))); err != nil {
		t.Fatalf("applyPatch: %v", err)
	}
	after := c.datafile.Load().ContextKinds
	if len(after) != len(before) {
		t.Fatalf("contextKinds count changed across a patch: before %d, after %d", len(before), len(after))
	}
	ck, ok := after["user"]
	if !ok || !ck.AvailableForRules || !ck.AvailableForExperiments {
		t.Fatalf("the user context kind should survive the patch unchanged, got %+v", after)
	}
}

// Over the wire: a complete but malformed patch frame is surfaced as an error
// yet does not kill the stream goroutine or drop the connection; a subsequent
// valid patch on the same connection still applies.
func TestStreamSurvivesMalformedPatchFrame(t *testing.T) {
	srv := newSSEServer()
	ts := httptest.NewServer(srv.handler())
	defer ts.Close()

	c := newStreamClient(t, ts.URL)
	sink := &errSink{}
	c.config.OnStreamError = sink.record
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	defer c.Close()

	srv.push(streamDatafile(1, false))
	waitFor(t, 2*time.Second, func() bool {
		df := c.datafile.Load()
		return df != nil && df.Version == 1
	})
	connBefore := srv.connects.Load()

	// A complete but malformed patch frame: reported and dropped, not fatal.
	srv.pushRaw("event: patch\nid: 2\ndata: { not valid json\n\n")
	// A valid patch on the same connection must still apply.
	srv.pushRaw(patchFrame(flipCheckout(1, 2, true)))
	waitFor(t, 2*time.Second, func() bool {
		return c.datafile.Load().Version == 2
	})
	if c.GetBooleanValue("checkout", false, ctxUser("u1", nil)) != true {
		t.Fatal("the valid patch after the malformed one should apply (true)")
	}
	if n := srv.connects.Load(); n != connBefore {
		t.Fatalf("a malformed frame must not drop/reconnect the stream: connects %d -> %d", connBefore, n)
	}
	if sink.count() == 0 {
		t.Fatal("the malformed patch frame should surface a decode error")
	}
}

// With -race: many concurrent Evaluate readers run while a writer walks the
// datafile forward one patch at a time, actively contending the flags-map swap.
func TestApplyPatchRaceWithConcurrentEvaluate(t *testing.T) {
	c := bareClient(t)
	if !c.adopt(streamDatafile(1, false)) {
		t.Fatal("seed datafile should be adopted")
	}

	const steps = 200
	const readers = 16
	stop := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = c.GetBooleanValue("checkout", false, ctxUser("u1", nil))
				}
			}
		}()
	}

	for v := int64(1); v <= steps; v++ {
		if err := c.applyPatch(mustJSON(flipCheckout(v, v+1, v%2 == 0))); err != nil {
			t.Fatalf("applyPatch %d->%d: %v", v, v+1, err)
		}
	}
	close(stop)
	wg.Wait()

	if got := c.datafile.Load().Version; got != steps+1 {
		t.Fatalf("writer should have reached version %d, got %d", steps+1, got)
	}
}
