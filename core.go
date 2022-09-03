package sparki

/*
sparki Copyright (C) 2022 Aurora McGinnis

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at https://mozilla.org/MPL/2.0/.
*/

import (
	"errors"
	"fmt"
	"go.uber.org/zap/zapcore"
)

type Core struct {
	zapcore.LevelEnabler
	sc     *StreamClient
	cfg    *coreOpts
	cf     bool
	fields map[string]interface{}
}

func (s *Core) With(fields []zapcore.Field) zapcore.Core {
	me := zapcore.NewMapObjectEncoder()
	for _, field := range fields {
		field.AddTo(me)
	}

	m := make(map[string]interface{}, len(s.fields)+len(fields))
	for k, v := range s.fields {
		m[k] = v
	}

	for k, v := range me.Fields {
		m[k] = v
	}

	return &Core{
		sc:     s.sc,
		cfg:    s.cfg,
		cf:     s.cf,
		fields: m,
	}
}

func (s *Core) Check(entry zapcore.Entry, checked *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if !s.cf && s.Enabled(entry.Level) {
		checked.AddCore(entry, s)
	}

	return checked
}

func (s *Core) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	if s.cf {
		return errors.New("core is closed")
	}

	fm := s.With(fields).(*Core).fields

	if s.cfg.includeCaller {
		fm["caller"] = fmt.Sprintf("%s:%s:%d", entry.Caller.File, entry.Caller.Function, entry.Caller.Line)
	}
	fm["level"] = entry.Level.String()

	if err := s.sc.Log(entry.Time, s.cfg.fieldFormatter(entry.Message, fm, entry.Stack)); err != nil {
		return err
	}

	if entry.Level > zapcore.ErrorLevel {
		// force a sync in case we're crashing
		return s.sc.Sync()
	}

	return nil
}

func (s *Core) Sync() error {
	if s.cf {
		return errors.New("core is closed")
	}

	return s.sc.Sync()
}

// NewCore creates a zapcore that logs to a given StreamClient.
// Use any number of CoreOption (from WithCore*) to configure the Core.
func NewCore(sc *StreamClient, opts ...CoreOption) *Core {
	cfg := defaultOptions()

	for _, opt := range opts {
		opt(cfg)
	}

	return &Core{cfg: cfg, sc: sc, fields: map[string]interface{}{}, LevelEnabler: cfg.level}
}
