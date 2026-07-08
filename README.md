<p align="center">
  <a href="https://feat.so">
    <img src="https://feat.so/logo/wordmark.png" alt="feat.so" width="320" />
  </a>
</p>

---

# feat Go SDK

Server-side Go SDK for [feat](https://feat.so) feature flags. Local flag evaluation against a live-streamed datafile, with polling as a safety net. Standard library only.

```
import "github.com/feathq/go-sdk/feat"
```

## Install

```bash
go get github.com/feathq/go-sdk
```

Go 1.23+.

## Usage

```go
package main

import (
    "context"
    "fmt"
    "os"

    "github.com/feathq/go-sdk/feat"
)

func main() {
    client, err := feat.NewClient(feat.Config{
        APIKey: os.Getenv("FEAT_SERVER_KEY"),
        URL:    "https://data-01.feat.so", // optional; this is the default
    })
    if err != nil {
        panic(err)
    }
    defer client.Close()

    client.Start(context.Background())
    if err := client.Ready(context.Background()); err != nil {
        panic(err)
    }

    evalCtx := feat.EvalContext{
        TargetingKey: "user-123",
        Kinds: map[string]feat.ContextKindObject{
            "user": {Key: "user-123", Attrs: map[string]any{
                "plan":  "pro",
                "email": "alice@example.com",
            }},
        },
    }
    fmt.Println("checkout-v2:", client.GetBooleanValue("checkout-v2", false, evalCtx))
}
```

Use a **server** API key (`feat_sdk_...`).

## How it works

- Fetches a per-environment datafile and keeps it in memory via `atomic.Pointer` for lock-free reads.
- **Streaming is on by default.** `Start(ctx)` holds a Server-Sent Events connection to the datafile stream endpoint and adopts each new datafile the instant it changes. Updates are version-ordered: a datafile is adopted only when its `version` is strictly greater than the one in memory.
- A background poll runs alongside the stream as a safety net on a two-tier cadence: while the stream is healthy it polls slowly (every 10 minutes) since the stream is the live path, and the instant the stream drops or is unreachable it reverts to the fast `PollInterval` and becomes the primary refresh path. The stream reconnects with exponential backoff.
- The fast poll runs every 30 seconds by default (configurable via `PollInterval`, floored at 5s); the slow safety-net cadence is never faster than `PollInterval`. ETag-aware via `If-None-Match`.
- Evaluation runs in-process: no per-flag network call.
- Set `DisableStreaming: true` to rely on polling alone.
- Both `Start(ctx)` background workers stop when `Close()` is called or `ctx` is cancelled.

## License

MIT
