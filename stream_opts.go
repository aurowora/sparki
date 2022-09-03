package sparki

/*
sparki Copyright (C) 2022 Aurora McGinnis

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at https://mozilla.org/MPL/2.0/.
*/

import "time"

// StreamOption can be passed to NewStream to configure an individual stream.
type StreamOption func(o *streamOpts)
type streamOpts struct {
	maxLogs int
	maxAge  time.Duration
}

// WithStreamMaxPayloadLength is like WithClientMaxPayloadLength, except that
// it only applies to a single stream
func WithStreamMaxPayloadLength(limit int) StreamOption {
	return func(o *streamOpts) {
		o.maxLogs = limit
	}
}

// WithStreamMaxPayloadAge is like WithClientMaxPayloadAge, except that it only applies
// to a single stream.
func WithStreamMaxPayloadAge(age time.Duration) StreamOption {
	return func(o *streamOpts) {
		o.maxAge = age
	}
}
