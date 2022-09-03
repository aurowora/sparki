package sparki

/*
sparki Copyright (C) 2022 Aurora McGinnis

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at https://mozilla.org/MPL/2.0/.
*/

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"
)

// initialMbufSize is the size of the slice for messages before any messages are pushed onto it
const initialMbufSize = 256

var mbp = &sync.Pool{New: func() interface{} { return make([][2]string, 0, initialMbufSize) }}

// stream represents a set of loki labels along with any messages that should be sent to loki
type stream struct {
	sync.Mutex
	labels   []Label
	messages [][2]string
	t        *time.Ticker
	cfg      *streamOpts
	seq      uint
}

// append adds a log message to the stream.
func (s *stream) append(t time.Time, message string) {
	s.messages = append(s.messages, [2]string{strconv.FormatInt(t.UnixNano(), 10), message})
}

// swap creates a new buffer and stores a pointer to it in messages. The old pointer is returned.
func (s *stream) swap() (old [][2]string) {
	old = s.messages
	s.messages = mbp.Get().([][2]string)
	return
}

// newStream creates a new stream
func newStream(cfg *streamOpts, labels []Label, t *time.Ticker) *stream {
	return &stream{
		labels:   labels,
		messages: mbp.Get().([][2]string),
		t:        t,
		cfg:      cfg,
	}
}

// StreamClient can write to a stream of a PushClient. Create one using the PushClient.NewStream method.
type StreamClient struct {
	sctx context.Context
	scf  context.CancelFunc
	ch   chan lokiMessage
}

// Log writes a message and its timestamp to the stream. Logs are buffered, so it may be some time (depending on
// the configuration) before it appears in Loki. This will block only if the underlying channel's buffer is full.
func (sc *StreamClient) Log(t time.Time, message string) error {
	if sc.sctx.Err() != nil {
		return errors.New("cannot write to a closed stream")
	}

	sc.ch <- lokiMessage{
		t:       t,
		message: message,
	}

	return nil
}

// Sync instructs PushClient to flush the stream to Loki.
// This will block until the logs have either been sent to loki or the push request has failed (in which case, the chunk
// will be retried in the background)
func (sc *StreamClient) Sync() error {
	if sc.sctx.Err() != nil {
		return errors.New("cannot sync a closed stream")
	}

	ch := make(chan struct{})
	sc.ch <- lokiMessage{sync: ch}
	<-ch

	return nil
}

// SyncNoWait is similar to Sync, except that it will not block at all.
func (sc *StreamClient) SyncNoWait() error {
	if sc.sctx.Err() != nil {
		return errors.New("cannot sync a closed stream")
	}

	sc.ch <- lokiMessage{sync: make(chan struct{})}

	return nil
}

// Close instructs the stream to close and then flush itself.
// This does not block, so messages will be flushed in the background.
func (sc *StreamClient) Close() error {
	if sc.sctx.Err() != nil {
		return errors.New("stream is already closed")
	}

	sc.scf()

	return nil
}
