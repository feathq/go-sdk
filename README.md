# go-sdk

Server-side Go SDK for [feat](https://feat.so) feature flags. Local flag evaluation against a polled datafile. Standard library only.

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
        APIKey:       os.Getenv("FEAT_SERVER_KEY"),
        DataPlaneURL: "https://data.feat.so",
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
- Polls every 30 seconds by default. ETag-aware via `If-None-Match`.
- Evaluation runs in-process: no per-flag network call.
- `Start(ctx)` spawns a goroutine that polls until `Close()` is called or `ctx` is cancelled.

## License

MIT
