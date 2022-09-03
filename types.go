package sparki

/*
sparki Copyright (C) 2022 Aurora McGinnis

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at https://mozilla.org/MPL/2.0/.
*/

import (
	"fmt"
	"time"
)

// Label associates a label name with a value. One or more labels uniquely identify a stream of messages
// in Loki. For best practices regarding labels in Loki, see: https://grafana.com/docs/loki/latest/best-practices/
type Label struct {
	Name  string
	Value string
}

type lokiMessage struct {
	sync    chan struct{}
	t       time.Time
	message string
}

// lokiPush is the loki api request body
type lokiPush struct {
	Streams []lokiStream `json:"streams"`
}

type lokiStream struct {
	Stream map[string]string `json:"stream"`
	Values [][2]string       `json:"values"`
	seq    uint
	sync   chan struct{}
}

// releaseSyncWaiters will unblock any go routines currently waiting for
// confirmation that a stream in this lokiPush has been flushed.
func (lp *lokiPush) releaseSyncWaiters() {
	for _, s := range lp.Streams {
		if s.sync != nil {
			close(s.sync)
		}
	}
}

// releaseMessageBuffers will return message buffers stored in lokiPush to the pool
func (lp *lokiPush) releaseMessageBuffers() {
	for _, s := range lp.Streams {
		s.Values = s.Values[:0]
		mbp.Put(s.Values)
	}
}

// httpError is produced when loki replies with an unexpected status code (typically non-204)
type httpError struct {
	statusCode int
	message    string
}

func (e *httpError) Error() string {
	d := e.message

	if len(d) > 128 {
		d = d[:128] + "..."
	}

	return fmt.Sprintf("unexpected http %d: %s", e.statusCode, d)
}
