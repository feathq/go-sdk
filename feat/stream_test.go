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

// streamDatafile builds a datafile carrying a single boolean flag "checkout"
// whose fallthrough variation is `on`. Evaluating the flag therefore reflects
// which datafile version is currently in memory.
func streamDatafile(version int64, on bool) *Datafile {
	flag := boolFlag()
	if on {
		flag.DefaultVariationID = ptr(trueVar.ID)
	}
	df := makeDatafile(map[string]FlagSpec{"checkout": flag}, nil)
	df.Version = version
	return df
}

func sseFrame(version int64, on bool) string {
	b, _ := json.Marshal(streamDatafile(version, on))
	return "event: put\nid: " + strconv.FormatInt(version, 10) + "\ndata: " + string(b) + "\n\n"
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
	streamErr atomic.Int32 // when non-zero, the stream endpoint replies with this status
	active    atomic.Int32 // currently open stream connections
	connects  atomic.Int32 // total stream connections accepted

	mu       sync.Mutex
	lastAuth string

	pollDF atomic.Pointer[Datafile]
}

func newSSEServer() *sseServer {
	return &sseServer{pushCh: make(chan *Datafile, 8)}
}

func (s *sseServer) push(df *Datafile) { s.pushCh <- df }
func (s *sseServer) dropConnection()   { s.pushCh <- nil }

func (s *sseServer) authHeader() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastAuth
}

func (s *sseServer) handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/sdk/v1/datafile/stream", func(w http.ResponseWriter, r *http.Request) {
		s.connects.Add(1)
		s.mu.Lock()
		s.lastAuth = r.Header.Get("Authorization")
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

		for {
			select {
			case <-r.Context().Done():
				return
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
