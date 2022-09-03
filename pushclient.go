package sparki

/*
sparki Copyright (C) 2022 Aurora McGinnis

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at https://mozilla.org/MPL/2.0/.
*/

import (
	"context"
	"sync"
	"time"
)

// PushClient is used for sending log lines to Loki.
// Use NewPushClient to create a new PushClient.
// Before the application terminates, call Close to ensure all logs are sent.
// PushClient is safe for concurrent use.
type PushClient struct {
	// The mutex is here to protect streams, everything else can be safely accessed without locking.
	m sync.Mutex
	// ctx, cf, wg are used to regulate shutdown
	ctx     context.Context
	cf      context.CancelFunc
	wg      *sync.WaitGroup
	pw      *pushWorkerPool
	cfg     *opts
	streams []*stream
}

// mchanBufSize is the size of the buffer for the message channel
const mchanBufSize = 128

func (l *PushClient) receiveMessage(s *stream, msg lokiMessage) {
	s.Lock()
	defer s.Unlock()

	if msg.sync != nil && len(s.messages) > 0 {
		l.pushStream(s, msg.sync)
		s.t.Reset(s.cfg.maxAge)
		return
	} else if msg.sync != nil {
		close(msg.sync)
		return
	}

	s.append(msg.t, msg.message)
	if len(s.messages) >= s.cfg.maxLogs {
		l.pushStream(s, nil)
		s.t.Reset(s.cfg.maxAge)
	}
}

func (l *PushClient) receiveMessageLoop(ch chan lokiMessage, s *stream) {
	defer l.wg.Done()

	for msg := range ch {
		l.receiveMessage(s, msg)
	}

	// ch is closed, push final messages and clean up
	l.m.Lock()
	defer l.m.Unlock()

	s.Lock()
	defer s.Unlock()

	s.t.Stop()
	if len(s.messages) > 0 {
		l.pushStream(s, nil)
	}

	v := make([]*stream, 0, len(l.streams))
	for _, a := range l.streams {
		if a != s {
			v = append(v, a)
		}
	}
	l.streams = v
	s.messages = s.messages[:0]
	mbp.Put(s.messages)
}

func (l *PushClient) checkMaxAge(ch <-chan time.Time, sctx context.Context, s *stream) {
	defer l.wg.Done()

	select {
	case <-ch:
		{
			s.Lock()
			if len(s.messages) > 0 {
				l.pushStream(s, nil)
			}
			s.t.Reset(s.cfg.maxAge)
			s.Unlock()
		}
	case <-sctx.Done():
		{
			break
		}
	}
}

// pushStream submits a stream to loki. The stream must be locked.
func (l *PushClient) pushStream(s *stream, syncFlag chan struct{}) {
	m := make(map[string]string)
	for _, lab := range s.labels {
		m[lab.Name] = lab.Value
	}

	p := lokiPush{Streams: []lokiStream{{Stream: m, Values: s.swap()}}}
	if l.cfg.testMode {
		p.Streams[0].seq = s.seq
		s.seq++
	}
	if syncFlag != nil {
		p.Streams[0].sync = syncFlag
	}

	l.pw.push(&p)
}

// NewStream creates a new stream with a given set of labels and returns a new StreamClient to write to the stream.
func (l *PushClient) NewStream(labels []Label, opts ...StreamOption) *StreamClient {
	if len(labels) == 0 {
		panic("at least 1 label is required")
	}
	if l.ctx.Err() != nil {
		panic("refusing to open new stream on closed LokiPushClient")
	}

	sopt := &streamOpts{
		maxLogs: l.cfg.maxPayloadLength,
		maxAge:  l.cfg.maxPayloadAge,
	}
	for _, opt := range opts {
		opt(sopt)
	}

	t := time.NewTicker(sopt.maxAge)
	s := newStream(sopt, labels, t)
	ch := make(chan lokiMessage, mchanBufSize)

	l.wg.Add(3)
	go l.receiveMessageLoop(ch, s)

	sctx, scf := context.WithCancel(l.ctx)
	sc := &StreamClient{
		ch:   ch,
		sctx: sctx,
		scf:  scf,
	}
	go func() {
		defer l.wg.Done()
		<-sctx.Done()
		close(sc.ch)
	}()
	go l.checkMaxAge(t.C, sctx, s)

	l.m.Lock()
	defer l.m.Unlock()
	l.streams = append(l.streams, s)

	return sc
}

// Close will shut down all open streams, sending their logs to Loki in the process.
// This will block until all streams have terminated cleanly. The PushClient cannot be used again.
func (l *PushClient) Close() error {
	l.cf()
	l.wg.Wait()
	l.pw.shutdown()
	return nil
}

// NewPushClient creates a new PushClient to send logs to endpoint with the specified Options
// The endpoint should be the Loki api route /loki/api/v1/push
func NewPushClient(endpoint string, opts ...ClientOption) *PushClient {
	o := defaultOpts()
	o.endpoint = endpoint
	for _, opt := range opts {
		opt(o)
	}

	ctx, cf := context.WithCancel(context.Background())
	pc := &PushClient{
		wg:      &sync.WaitGroup{},
		ctx:     ctx,
		cf:      cf,
		cfg:     o,
		pw:      startWorkerPool(o),
		streams: make([]*stream, 0, 1),
	}

	return pc
}
