package sparki

/*
sparki Copyright (C) 2022 Aurora McGinnis

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at https://mozilla.org/MPL/2.0/.
*/

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/goccy/go-json"
	"github.com/klauspost/compress/gzip"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// pushChannelBufSize is the number of elements for the pushWorkerPool internal buffer
const pushChannelBufSize = 32

var rbp = &sync.Pool{New: func() interface{} { return new(bytes.Buffer) }}

type pushWorkerPool struct {
	*sync.WaitGroup
	ch  chan *lokiPush
	cfg *opts
	hcl *http.Client
	// flock protects failed
	flock            *sync.Mutex
	failed           []*pushRequest
	stopFailedWorker context.CancelFunc
	failedWorkerDone chan struct{}
}

func (p *pushWorkerPool) marshalRequest(lp *lokiPush) (*pushRequest, error) {
	// marshal the body. Loki accepts either protocol buffers compressed with snappy
	// or JSON compressed with GZIP. JSON+GZIP is the easier of the two.
	buf := rbp.Get().(*bytes.Buffer)
	defer rbp.Put(buf)
	defer buf.Reset()

	gz, _ := gzip.NewWriterLevel(buf, 3)
	enc := json.NewEncoder(gz)
	if err := enc.Encode(lp); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}

	// build up headers
	var headers http.Header
	if p.cfg.requestHeaders != nil {
		headers = p.cfg.requestHeaders.Clone()
	} else {
		headers = make(http.Header, 3)
	}
	headers.Set("Content-Type", "application/json")
	headers.Set("Content-Encoding", "gzip")
	headers.Set("Content-Length", strconv.Itoa(buf.Len()))

	// This header is used internally for testing. It will never be sent
	// to an actual loki server.
	targetFailures := 0
	if p.cfg.testMode {
		sc := ""
		for idx, s := range lp.Streams {
			sc += fmt.Sprint(s.seq)
			if idx+1 < len(lp.Streams) {
				sc += ", "
			}
		}
		headers.Set("X-StreamSequences", sc)

		// similarly, this is used to simulate failures in test mode
		if v, ok := lp.Streams[0].Stream["failCount"]; ok {
			var err error
			targetFailures, err = strconv.Atoi(v)
			if err != nil {
				return nil, err
			}
		}
	}

	bodyBytes := make([]byte, buf.Len())
	if _, err := buf.Read(bodyBytes); err != nil {
		return nil, err
	}

	return &pushRequest{
		body:               bodyBytes,
		headers:            headers,
		testTargetFailures: targetFailures,
	}, nil
}

// sendRequest attempts to send a pushRequest to Loki.
func (p *pushWorkerPool) sendRequest(pr *pushRequest) error {
	if p.cfg.testMode && pr.testTargetFailures > pr.failures {
		return errors.New("fake failure for testing")
	}

	ctx, cf := context.WithTimeout(context.Background(), 30*time.Second)
	defer cf()

	// bytes.Reader won't modify the underlying slice, so if we fail we can just retry.
	r := bytes.NewReader(pr.body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.cfg.endpoint, r)
	if err != nil {
		return err
	}
	req.Header = pr.headers

	resp, err := p.hcl.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode >= 200 && resp.StatusCode <= 299 {
		return nil
	}

	// we got an error from the server, attempt to read the body for more info
	mTextBuilder := new(strings.Builder)
	if _, err := io.Copy(mTextBuilder, resp.Body); err != nil {
		mTextBuilder.Reset()
		mTextBuilder.WriteString("http body text not available: ")
		mTextBuilder.WriteString(err.Error())
	}

	return &httpError{
		statusCode: resp.StatusCode,
		message:    mTextBuilder.String(),
	}
}

func (p *pushWorkerPool) handleRequestFailure(err error, m *pushRequest) {
	m.failures++
	if m.failures-1 > p.cfg.retryLimit {
		fmt.Printf("giving up on log batch after %d failures\n", m.failures)
		return
	}
	m.lastFailure = time.Now()

	fmt.Printf("log batch send failed (tries: %d, last failed: %v): %v\n", m.failures, m.lastFailure, err)

	p.flock.Lock()
	defer p.flock.Unlock()
	if p.failed == nil {
		fmt.Printf("worker pool has shut down, abandoning failed chunk after %d failures\n", m.failures)
		return
	}
	p.failed = append(p.failed, m)
}

// worker receives requests from a PushClient and dispatches them to loki
func (p *pushWorkerPool) worker() {
	defer p.Done()

	for task := range p.ch {
		m, err := p.marshalRequest(task)
		if err != nil {
			// a request marshal failure is unlikely to be resolved
			// by trying again. We will give up on this chunk.
			task.releaseSyncWaiters()
			task.releaseMessageBuffers()
			if p.cfg.testMode {
				p.cfg.errorCb(err)
			}
			fmt.Printf("failed to marshal log batch, abandoning chunk: %v\n", err)
			continue
		}

		err = p.sendRequest(m)
		if err != nil {
			task.releaseSyncWaiters()
			task.releaseMessageBuffers()
			if p.cfg.testMode {
				p.cfg.errorCb(err)
			}
			p.handleRequestFailure(err, m)
			continue
		}

		task.releaseSyncWaiters()
		task.releaseMessageBuffers()
	}
}

// retryFailedWorker retries any failed requests sent to it by a worker or
// itself (via a request failing multiple times)
func (p *pushWorkerPool) retryFailedWorker(ctx context.Context) {
	defer close(p.failedWorkerDone)

	retryFailedTasks := func(t []*pushRequest) {
		for _, task := range t {
			err := p.sendRequest(task)
			if err != nil {
				if p.cfg.testMode {
					p.cfg.errorCb(err)
				}
				p.handleRequestFailure(err, task)
				continue
			}
		}
	}

	for {
		if ctx.Err() != nil {
			break
		}

		p.flock.Lock()
		retryableTasks := make([]*pushRequest, 0, 2)
		unretryableTasks := make([]*pushRequest, 0, len(p.failed))
		for _, task := range p.failed {
			t := task.lastFailure.Add(time.Second * time.Duration(1<<task.failures))
			if t.Before(time.Now()) {
				retryableTasks = append(retryableTasks, task)
			} else {
				unretryableTasks = append(unretryableTasks, task)
			}
		}
		p.failed = unretryableTasks
		p.flock.Unlock()
		retryFailedTasks(retryableTasks)

		time.Sleep(2 * time.Second)
	}

	// we're shutting down now, retry all tasks one last time
	p.flock.Lock()
	taskCpy := make([]*pushRequest, len(p.failed))
	for idx, t := range p.failed {
		taskCpy[idx] = t
	}
	p.failed = nil
	p.flock.Unlock()

	retryFailedTasks(taskCpy)
}

// push arranges for a batch of log messages to be sent to loki
func (p *pushWorkerPool) push(batch *lokiPush) {
	p.ch <- batch
}

// shutdown sends whatever remains and shuts down the channels. Attempting to use pushWorkerPool after
// shutdown will result in a panic. Shutdown will block until all go routines have stopped.
func (p *pushWorkerPool) shutdown() {
	close(p.ch)
	p.Wait()
	p.stopFailedWorker()
	<-p.failedWorkerDone
}

func startWorkerPool(cfg *opts) *pushWorkerPool {
	ctx, cf := context.WithCancel(context.Background())

	p := &pushWorkerPool{
		WaitGroup: &sync.WaitGroup{},
		ch:        make(chan *lokiPush, pushChannelBufSize),
		cfg:       cfg,
		hcl: &http.Client{
			Transport: cfg.transport,
		},
		flock:            &sync.Mutex{},
		failed:           make([]*pushRequest, 0, 8),
		stopFailedWorker: cf,
		failedWorkerDone: make(chan struct{}),
	}
	p.Add(p.cfg.numWorkers)
	for i := 0; i < p.cfg.numWorkers; i++ {
		go p.worker()
	}
	go p.retryFailedWorker(ctx)

	return p
}

// pushRequest represents a request that is ready to be sent
type pushRequest struct {
	body               []byte
	headers            http.Header
	failures           int
	lastFailure        time.Time
	testTargetFailures int
}
