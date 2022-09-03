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
	"encoding/base64"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/goccy/go-json"
	"github.com/klauspost/compress/gzip"
	"io"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type testRequirements struct {
	msgThreshold int
	maxTime      int
	numTries     *int
	streamCounts map[string][]int64
	minTime      time.Time
	ecb          func(err error)
}

// define an additional option
func withTestMode(ec func(err error)) ClientOption {
	return func(o *opts) {
		o.errorCb = ec
		o.testMode = true
	}
}

const testPort = 53335
const testMagicMessage = "Hello, this is a test message!"

func createTestServer() *gin.Engine {
	router := gin.Default()
	router.POST("/api/v1/loki/push", func(c *gin.Context) {
		if c.GetHeader("Content-Type") != "application/json" {
			_ = c.AbortWithError(400, errors.New("json required"))
			return
		}

		buf := new(bytes.Buffer)
		if strings.Contains(c.GetHeader("Content-Encoding"), "gzip") {
			gr, err := gzip.NewReader(c.Request.Body)
			if err != nil {
				_ = c.AbortWithError(400, err)
				return
			}

			if _, err = io.Copy(buf, gr); err != nil {
				_ = c.AbortWithError(400, err)
				return
			}
			if err = gr.Close(); err != nil {
				_ = c.AbortWithError(400, err)
				return
			}
		} else {
			if _, err := io.Copy(buf, c.Request.Body); err != nil {
				_ = c.AbortWithError(400, err)
				return
			}
		}

		var lp lokiPush
		if err := json.Unmarshal(buf.Bytes(), &lp); err != nil {
			_ = c.AbortWithError(400, err)
			return
		}

		vf := false

		// Do some validation work
		if len(lp.Streams) < 1 {
			_ = c.AbortWithError(400, errors.New("at least 1 stream is required"))
			return
		}

		var estreams map[string][]int64
		if esh := c.GetHeader("X-ExpectedStreams"); esh != "" {
			bo, err := base64.URLEncoding.DecodeString(esh)
			if err != nil {
				_ = c.AbortWithError(400, err)
				return
			}
			dec := gob.NewDecoder(bytes.NewReader([]byte(bo)))
			err = dec.Decode(&estreams)
			if err != nil {
				_ = c.AbortWithError(400, err)
				return
			}
		} else {
			_ = c.AbortWithError(400, errors.New("missing X-ExpectedStreams"))
			return
		}

		sequences := make([]int64, 0)
		if sh := c.GetHeader("X-StreamSequences"); sh != "" {
			s := strings.Split(sh, ",")
			for _, x := range s {
				num, err := strconv.ParseInt(strings.TrimSpace(x), 10, 64)
				if err != nil {
					_ = c.AbortWithError(400, err)
					return
				}
				sequences = append(sequences, num)
			}
		} else {
			_ = c.AbortWithError(400, errors.New("missing X-StreamSequences"))
			return
		}

		if len(lp.Streams) != len(sequences) {
			_ = c.AbortWithError(400, errors.New("X-StreamSequences does not match body"))
			return
		}

		for idx, s := range lp.Streams {
			if len(s.Stream) < 1 {
				_ = c.Error(fmt.Errorf("stream %d requires at least 1 label", idx))
				vf = true
			}

			for k, v := range s.Stream {
				if k == "" || v == "" {
					_ = c.Error(fmt.Errorf("label of stream %d has empty key or value: %s=%s", idx, k, v))
					vf = true
				}
			}

			if len(s.Values) < 1 {
				_ = c.Error(fmt.Errorf("stream %d requires at least 1 log message", idx))
				vf = true
			}

			lab, ok := s.Stream["test"]
			if !ok {
				_ = c.Error(fmt.Errorf("stream %d missing test label", idx))
				vf = true
			}

			if lab != "" && estreams != nil && int64(len(s.Values)) != estreams[lab][sequences[idx]] {
				_ = c.Error(fmt.Errorf("stream %d expected %d messages, but sent %d", idx, estreams[lab][sequences[idx]], len(s.Values)))
				vf = true
			}

			lastNano := time.Time{}
			eti, err := strconv.ParseInt(c.GetHeader("X-ExpectedMinTime"), 10, 64)
			if err == nil {
				lastNano = time.Unix(0, eti)
			}
			for midx, m := range s.Values {
				var mt time.Time
				mti, err := strconv.ParseInt(m[0], 10, 64)
				if err != nil {
					_ = c.Error(fmt.Errorf("message %d of stream %d has bad t format of %s", midx, idx, m[0]))
					vf = true
					continue
				}
				mt = time.Unix(0, mti)

				if !lastNano.Before(mt) && !lastNano.Equal(mt) {
					_ = c.Error(fmt.Errorf("message %d of stream %d was sent out of order or with wrong t (message t = %v, previous t = %v)", midx, idx, mt, lastNano))
					vf = true
					continue
				}
				lastNano = mt

				if m[1] != testMagicMessage {
					_ = c.Error(fmt.Errorf("message %d of stream %d was sent with incorrect message contents\nactual: %s\nexpected: %s", midx, idx, m[1], testMagicMessage))
				}
			}
		}

		if vf {
			errs := make([]*gin.Error, 0, len(c.Errors))
			for _, err := range c.Errors {
				errs = append(errs, err)
			}
			c.JSON(400, errs)
			return
		} else {
			c.AbortWithStatus(204)
			return
		}
	})
	return router
}

func createTestingPushClient(treq *testRequirements) (*PushClient, func() []error) {
	lokiSrv := createTestServer()

	headerDict := make(http.Header, 2)
	if treq.streamCounts != nil {
		g := new(bytes.Buffer)
		enc := gob.NewEncoder(g)
		err := enc.Encode(treq.streamCounts)
		if err != nil {
			panic(err)
		}
		b64 := base64.URLEncoding.EncodeToString(g.Bytes())

		headerDict.Set("X-ExpectedStreams", b64)
	}
	headerDict.Set("X-ExpectedMinTime", fmt.Sprint(treq.minTime.UnixNano()))

	opts := make([]ClientOption, 0, 5)
	opts = append(opts, WithClientHeaders(headerDict))
	if treq.msgThreshold > 0 {
		opts = append(opts, WithClientMaxPayloadLength(treq.msgThreshold))
	}
	if treq.maxTime > 0 {
		opts = append(opts, WithClientMaxPayloadAge(time.Second*time.Duration(treq.maxTime)))
	} else {
		opts = append(opts, WithClientMaxPayloadAge(time.Hour*24)) // dont flush by default
	}
	if treq.numTries != nil && *treq.numTries >= 0 {
		opts = append(opts, WithClientRetryLimit(*treq.numTries))
	}
	if treq.ecb != nil {
		opts = append(opts, withTestMode(treq.ecb))
	}

	lpc := NewPushClient(fmt.Sprintf("http://127.0.0.1:%d/api/v1/loki/push", testPort), opts...)
	srv := &http.Server{
		Addr:    fmt.Sprintf("127.0.0.1:%d", testPort),
		Handler: lokiSrv,
	}

	go func() {
		if err := srv.ListenAndServe(); errors.Is(err, http.ErrServerClosed) {
			return
		} else if err != nil {
			panic(err)
		}
	}()
	time.Sleep(250 * time.Millisecond)

	closer := func() (ret []error) {
		ctx, cf := context.WithTimeout(context.Background(), 1*time.Minute)
		defer cf()

		ec := make(chan error, 1)
		go func() {
			defer close(ec)
			err := lpc.Close()
			if err != nil {
				ec <- err
			}
		}()

		select {
		case <-ctx.Done():
			{
				ret = append(ret, ctx.Err())
				break
			}
		case err := <-ec:
			{
				if err != nil {
					ret = append(ret, err)
				}
				break
			}
		}

		ctx2, cf2 := context.WithTimeout(context.Background(), 30*time.Second)
		defer cf2()
		err := srv.Shutdown(ctx2)
		if err != nil {
			ret = append(ret, err)
		}

		return
	}

	return lpc, closer
}

type errorReporter struct {
	m    sync.Mutex
	errs []error
}

func (e *errorReporter) reportError(err error) {
	e.m.Lock()
	defer e.m.Unlock()

	e.errs = append(e.errs, err)

	if herr, ok := err.(*httpError); ok {
		var jd = make([]map[string]interface{}, 0)
		if uerr := json.Unmarshal([]byte(herr.message), &jd); uerr == nil {
			for _, emap := range jd {
				if eval, ok := emap["error"]; ok {
					e.errs = append(e.errs, fmt.Errorf("%v", eval))
				}
			}
		}
	}
}

func (e *errorReporter) getReported() []error {
	e.m.Lock()
	defer e.m.Unlock()
	defer func() {
		e.errs = make([]error, 0, 1)
	}()

	return e.errs
}

var zeroTries = 0

func TestBasic(t *testing.T) {
	errRep := &errorReporter{}
	lpc, closer := createTestingPushClient(&testRequirements{
		numTries:     &zeroTries,
		streamCounts: map[string][]int64{"1": {5}},
		minTime:      time.Now(),
		ecb:          errRep.reportError,
	})

	s := lpc.NewStream([]Label{{Name: "test", Value: "1"}})

	for i := 0; i < 5; i++ {
		if err := s.Log(time.Now(), testMagicMessage); err != nil {
			t.Error(err)
		}
	}

	for _, err := range closer() {
		t.Errorf("error during close: %v", err)
	}

	errs := errRep.getReported()
	for _, err := range errs {
		t.Errorf("error during test: %v", err)
	}
}

func TestSync(t *testing.T) {
	errRep := &errorReporter{}
	lpc, closer := createTestingPushClient(&testRequirements{
		numTries:     &zeroTries,
		streamCounts: map[string][]int64{"1": {1, 2}},
		minTime:      time.Now(),
		ecb:          errRep.reportError,
	})

	s := lpc.NewStream([]Label{{Name: "test", Value: "1"}})
	if err := s.Log(time.Now(), testMagicMessage); err != nil {
		t.Error(err)
	}
	if err := s.Sync(); err != nil {
		t.Error(err)
	}
	if err := s.Log(time.Now(), testMagicMessage); err != nil {
		t.Error(err)
	}
	if err := s.Log(time.Now(), testMagicMessage); err != nil {
		t.Error(err)
	}

	for _, err := range closer() {
		t.Errorf("error during close: %v", err)
	}

	errs := errRep.getReported()
	for _, err := range errs {
		t.Errorf("error during test: %v", err)
	}
}

func TestMaxLogFlush(t *testing.T) {
	errRep := &errorReporter{}
	lpc, closer := createTestingPushClient(&testRequirements{
		numTries:     &zeroTries,
		streamCounts: map[string][]int64{"1": {defaultMaxPayloadLength, 1}},
		minTime:      time.Now(),
		ecb:          errRep.reportError,
	})

	s := lpc.NewStream([]Label{{Name: "test", Value: "1"}})
	for i := 0; i < defaultMaxPayloadLength+1; i++ {
		s.Log(time.Now(), testMagicMessage)
	}

	for _, err := range closer() {
		t.Errorf("error during close: %v", err)
	}

	errs := errRep.getReported()
	for _, err := range errs {
		t.Errorf("error during test: %v", err)
	}
}

func TestMaxAgeFlush(t *testing.T) {
	errRep := &errorReporter{}
	lpc, closer := createTestingPushClient(&testRequirements{
		numTries:     &zeroTries,
		streamCounts: map[string][]int64{"1": {5}},
		minTime:      time.Now(),
		maxTime:      2,
		ecb:          errRep.reportError,
	})

	s := lpc.NewStream([]Label{{Name: "test", Value: "1"}})
	for i := 0; i < 5; i++ {
		s.Log(time.Now(), testMagicMessage)
	}

	time.Sleep(5 * time.Second)
	fmt.Println("(The previous log lines should contain an HTTP push log)")

	for _, err := range closer() {
		t.Errorf("error during close: %v", err)
	}

	errs := errRep.getReported()
	for _, err := range errs {
		t.Errorf("error during test: %v", err)
	}
}

var threeTries = 3

func TestRetries(t *testing.T) {
	errRep := &errorReporter{}
	lpc, closer := createTestingPushClient(&testRequirements{
		numTries:     &threeTries,
		streamCounts: map[string][]int64{"1": {5}},
		minTime:      time.Now(),
		ecb:          errRep.reportError,
	})

	s := lpc.NewStream([]Label{{Name: "test", Value: "1"}, {Name: "failCount", Value: "3"}})

	for i := 0; i < 5; i++ {
		if err := s.Log(time.Now(), testMagicMessage); err != nil {
			t.Error(err)
		}
	}
	_ = s.SyncNoWait()
	time.Sleep(time.Second * 20)

	for _, err := range closer() {
		t.Errorf("error during close: %v", err)
	}

	errs := errRep.getReported()

	if len(errs) < 3 {
		t.Errorf("expected at least 3 errors, got %d", len(errs))
	}

	for idx, err := range errs {
		if idx < 3 && err.Error() != "fake failure for testing" {
			t.Errorf("expected faked failure at %d, got %v", idx, err)
		} else if idx >= 3 {
			t.Errorf("error during test: %v", err)
		}
	}
}

func TestMultipleStreams(t *testing.T) {
	nt := runtime.NumCPU() * 8
	scounts := make(map[string][]int64, nt)
	for i := 0; i < nt; i++ {
		scounts[fmt.Sprint(i)] = []int64{defaultMaxPayloadLength, 1, defaultMaxPayloadLength, 1, 1}
	}

	errRep := &errorReporter{}
	lpc, closer := createTestingPushClient(&testRequirements{
		numTries:     &zeroTries,
		streamCounts: scounts,
		minTime:      time.Now(),
		ecb:          errRep.reportError,
	})

	wg := &sync.WaitGroup{}
	wg.Add(nt)

	for i := 0; i < nt; i++ {
		go func(n int) {
			defer wg.Done()

			sc := lpc.NewStream([]Label{{Name: "test", Value: fmt.Sprint(n)}})

			for x := 0; x < defaultMaxPayloadLength+1; x++ {
				_ = sc.Log(time.Now(), testMagicMessage)
			}
			_ = sc.Sync()
			for x := 0; x < defaultMaxPayloadLength+1; x++ {
				_ = sc.Log(time.Now(), testMagicMessage)
			}
			_ = sc.Sync()
			_ = sc.Log(time.Now(), testMagicMessage)
		}(i)
	}

	wg.Wait()

	for _, err := range closer() {
		t.Errorf("error during close: %v", err)
	}

	errs := errRep.getReported()
	for _, err := range errs {
		t.Errorf("error during test: %v", err)
	}
}
