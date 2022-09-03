package sparki

/*
sparki Copyright (C) 2022 Aurora McGinnis

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at https://mozilla.org/MPL/2.0/.
*/

import (
	"fmt"
	"go.uber.org/zap/zapcore"
	"strings"
	"sync"
)

// CoreOption can be passed to NewCore to configure the zap core
type CoreOption func(*coreOpts)

// FieldFormatter takes a log message and its corresponding fields and returns a string with the fields and message combined somehow.
type FieldFormatter func(message string, fields map[string]interface{}, stack string) string

type coreOpts struct {
	fieldFormatter FieldFormatter
	level          zapcore.Level
	includeCaller  bool
}

var sbf = &sync.Pool{New: func() interface{} { return new(strings.Builder) }}

func defaultOptions() *coreOpts {
	return &coreOpts{
		fieldFormatter: func(message string, fields map[string]interface{}, stack string) string {
			b := sbf.Get().(*strings.Builder)
			defer sbf.Put(b)
			defer b.Reset()

			b.WriteString(message)
			b.WriteString(" :: ")

			i := 0
			for k, v := range fields {
				b.WriteString(fmt.Sprintf("%s=%v", k, v))
				i++
				if i < len(fields) {
					b.WriteString(" ")
				}
			}

			if stack != "" {
				b.WriteString("\n")
				b.WriteString(stack)
			}

			return b.String()
		},
		level:         zapcore.InfoLevel,
		includeCaller: true,
	}
}

// WithCoreFieldFormatter allows a function that is responsible for combining the log message and the fields into a single
// string to be specified. The default field formatter separates the message and the field list with "::" and lists
// the fields and their values, separated by spaces. E.g. My message :: level="error" field1="a" field2="b"
func WithCoreFieldFormatter(formatter FieldFormatter) CoreOption {
	return func(o *coreOpts) {
		o.fieldFormatter = formatter
	}
}

// WithCoreLevel specifies the log level. The default is INFO or higher.
func WithCoreLevel(level zapcore.Level) CoreOption {
	return func(o *coreOpts) {
		o.level = level
	}
}

// WithCoreCaller specifies whether to add caller information to the log message as a field
// if zap provides the information
func WithCoreCaller(includeCaller bool) CoreOption {
	return func(o *coreOpts) {
		o.includeCaller = includeCaller
	}
}
