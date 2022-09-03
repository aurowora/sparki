package sparki

/*
sparki Copyright (C) 2022 Aurora McGinnis

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at https://mozilla.org/MPL/2.0/.
*/

import (
	"net/http"
	"time"
)

// ClientOption is used to configure a PushClient in NewPushClient
// See the With* functions for the available options.
type ClientOption func(*opts)

type opts struct {
	endpoint         string
	maxPayloadLength int // in log lines
	maxPayloadAge    time.Duration
	requestHeaders   http.Header
	transport        http.RoundTripper
	retryLimit       int
	numWorkers       int             // number of HTTP push workers to use
	errorCb          func(err error) // errorCb is used internally for testing. This can be called from any go routine
	testMode         bool            // if set, will generate some extra headers that get intercepted by the test http server
}

const (
	defaultMaxPayloadLength = 1 << 13
	defaultMaxPayloadAge    = time.Duration(5) * time.Minute
	defaultMaxRetries       = 7
)

func defaultOpts() *opts {
	return &opts{
		maxPayloadLength: defaultMaxPayloadLength,
		maxPayloadAge:    defaultMaxPayloadAge,
		retryLimit:       defaultMaxRetries,
		numWorkers:       6,
		errorCb:          func(err error) {}, // default error reporter does nothing
	}
}

// WithClientMaxPayloadLength specifies the number of log lines that can be added to a stream
// before it must be committed to Loki. Note that logs may be sent to loki before this
// threshold is hit (requested sync or maxPayloadAge).
func WithClientMaxPayloadLength(logLines int) ClientOption {
	if logLines <= 0 {
		panic("argument to WithMaxPayloadLength must be positive")
	}

	return func(o *opts) {
		o.maxPayloadLength = logLines
	}
}

// WithClientMaxPayloadAge specifies the maximum amount of t that may pass before a given stream must
// be committed to Loki. Note that logs may be sent to loki more frequently (requested sync or maxPayloadLength).
func WithClientMaxPayloadAge(d time.Duration) ClientOption {
	if d.Nanoseconds() <= 0 {
		panic("argument to WithMaxPayloadAge must be non-zero")
	}

	return func(o *opts) {
		o.maxPayloadAge = d
	}
}

// WithClientHeaders allows you to specify HTTP headers that should be added to any requests to Loki.
// This is potentially useful for Authorization purposes.
func WithClientHeaders(h http.Header) ClientOption {
	if h == nil {
		panic("argument to WithHeaders must not be nil")
	}

	return func(o *opts) {
		o.requestHeaders = h
	}
}

// WithClientTransport allows you to specify a custom HTTP transport that should be used when communicating with
// Loki.
func WithClientTransport(t http.RoundTripper) ClientOption {
	if t == nil {
		panic("argument to WithTransport must not be nil")
	}

	return func(o *opts) {
		o.transport = t
	}
}

// WithClientRetryLimit controls how many times logs that fail will be retried.
// Backoff period after a failure is exponential.
// PushClient gives up on logs after the limit is passed. Logs are lost at this point.
func WithClientRetryLimit(limit int) ClientOption {
	if limit < 0 {
		panic("argument to WithRetryLimit must not be negative")
	}

	return func(o *opts) {
		o.retryLimit = limit
	}
}

// WithClientPushWorkers sets the number of go routines created to send HTTP requests to Loki. The default is 6 workers.
func WithClientPushWorkers(numWorkers int) ClientOption {
	if numWorkers <= 0 {
		panic("at least 1 push worker is required")
	}
	return func(o *opts) {
		o.numWorkers = numWorkers
	}
}
