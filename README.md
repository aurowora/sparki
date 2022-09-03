# Sparki
Sparki provides a client for sending logs to Loki from [Zap](https://github.com/uber-go/zap) and Golang.

![Loki Logs](loki.png)

Sparki provides a core for use with Zap:

```go
package main

import "github.com/uber-go/zap"
import "github.com/uber-go/zapcore"
import "github.com/aurowora/sparki"

func main() {
    // Create a PushClient
    pushClient := sparki.NewPushClient("https://loki.example.com/loki/api/v1/push")
    defer pushClient.Close() // Closing the push client will flush all logs
    
    // Open a stream
    streamClient := pushClient.NewStream([]sparki.Label{{Name: "source", Value: "sparki"}})
    // defer streamClient.Close() is redundant because pushClient.Close will close all streams
    
    // Create a core for use with Zap, create a zap logger, and attach the core
    sparkiCore := sparki.NewCore(streamClient)
    logger := zap.NewExample()
    defer logger.Sync()
    logger = logger.WithOptions(zap.WrapCore(func (core zapcore.Core) zapcore.Core {
        return zapcore.NewTee(core, sparkiCore)
    }))
    
    // Log some messages
    logger.Info("Hello, world! ⚡")
    logger.Error("Crashing and burning 🔥", zap.String("scream", "aaaah!"))
}
```

Sparki's PushClient and StreamClient can also be used directly, without Zap.

```go
package main

import "time"
import "github.com/aurowora/sparki"

func main() {
    pushClient := sparki.NewPushClient("https://loki.example.com/loki/api/v1/push")
    defer pushClient.Close()
    
    streamClient := pushClient.NewStream([]sparki.Label{{Name: "source", Value: "sparki"}})
    streamClient.Log(time.Now(), "This will be sent to Loki.")
}
```
## Configuring

Sparki provides configuration options for its various types.

### ClientOption

One or more ClientOptions can be passed to sparki.NewPushClient to configure it. ClientOptions are returned by functions
beginning with `WithClient`. The following options are available:

- WithClientMaxPayloadLength(int) - Specifies the maximum number of logs to store in memory (per stream) before the logs must be sent to Loki. Defaults to 8192.
- WithClientMaxPayloadAge(time.Duration) - Specifies the maximum amount of time that a log can remain in memory before it must be sent to Loki. Defaults to 5 minutes.
- WithClientHeaders(http.Header) - Specifies any additional headers to be sent to Loki. Can be used to add headers for authentication.
- WithClientTransport(http.RoundTripper) - Specifies the Transport that should be used for the HTTP client.
- WithClientRetryLimit(int) - Specifies how many times PushClient is allowed to retry failures when sending logs. By default, Sparki may retry up to 7 times before the logs are lost for good.
- WithClientPushWorkers(int) - Specifies how many go routines are created to send HTTP requests to Loki. By default, 6 workers are used.

#### Authentication

It is common for Loki to reside behind a reverse proxy that requires authentication of some sort.

An example of using `WithClientHeaders` to authenticate with a reverse proxy that uses a header-based auth scheme:

```go
package main

import "net/http"
import "github.com/aurowora/sparki"

func main() {
    extraHeaders := make(http.Header, 1)
    extraHeaders.Set("Authorization", "Basic YWxhZGRpbjpvcGVuc2VzYW1l")
    
    pushClient := sparki.NewPushClient("https://loki.example.com/loki/api/v1/push", sparki.WithClientHeaders(extraHeaders))
    defer pushClient.Close()
    
    // other setup code...
}
```

An example of using `WithClientTransport` to authenticate with a reverse proxy that uses an mTLS-based auth scheme:

```go
package main

import "crypto/tls"
import "net/http"
import "github.com/aurowora/sparki"

func main() {
    cert, err := tls.LoadX509KeyPair("./client.pem", "./client.key")
    if err != nil {
        panic(err)
    }
    
    transport := http.Transport{
        TLSClientConfig: &tls.Config{
            Certificates: []tls.Certificate{cert},
        },
    }
    
    pushClient := sparki.NewPushClient("https://loki.example.com/loki/api/v1/push", sparki.WithClientTransport(transport))
    defer pushClient.Close()
    
    // other setup code...
}
```

### StreamOption

One or more StreamOptions can be passed to PushClient's NewStream method to configure the stream. StreamOptions are returned by functions beginning
with `WithStream`. The following options are available:

- WithStreamMaxPayloadLength(int) - Like WithClientMaxPayloadLength, but for this stream only. Defaults to the client's max payload length.
- WithStreamMaxPayloadAge(time.Duration) - Like WithClientMaxPayloadAge, but for this stream only. Defaults the client's max payload age.

### CoreOption

One or more CoreOptions can be passed to sparki.NewCore to configure it. CoreOptions are returned by functions beginning
with `WithCore`. The following options are available:

- WithCoreFieldFormatter(sparki.FieldFormatter) - Specifies a function used to format log messages and fields into the message string.
- WithCoreLevel(zapcore.Level) - Specifies the log level that this core should use. Defaults to zapcore.InfoLevel
- WithCoreCaller(bool) - Specifies whether to inject a `caller` field containing information about the function making the log call. Defaults to true.

## Labels

Loki does poorly with high cardinality logs (having many labels with many potential values) and works best with a small
set of static labels. High label cardinality results in many small streams being stored, which can bloat Loki's index size
and slow down queries. As such, our API favors the use of static labels. Please read the Loki [best practices](https://grafana.com/docs/loki/latest/best-practices/) for more information.

One effect of this is that the implementation of `sparki.Core` appends any fields to the log line itself using the function
that is provided in `WithCoreFieldFormatter` (or the default implementation if none provided). For similar reasons, instead of using labels
for the log level, we opt to add a field called `level` for each message. This means that when querying your logs, you'd need to use
a query like the following query to filter for a specific log level:

```
{app="myApp"} |= `level=error`
```

Zap provides the following levels: `debug`, `info`, `warn`, `error`, `dpanic`, `panic`, and `fatal`. Any other field can be queried in a similar manner.

Currently, neither the Zap core nor the PushClient itself support the use of dynamic labels, though this may change if
I ever find a use for them for my own projects.

## Tests

Tests cover most things in the package other than the Zap core, which I've been testing by hand until I have time to write
proper tests for it. Tests can be ran using `go test`.

## License

```
sparki Copyright (C) 2022 Aurora McGinnis

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at https://mozilla.org/MPL/2.0/.
```

The full terms of the Mozilla Public License 2.0 can also be found in the `LICENSE.txt` file within this repository.