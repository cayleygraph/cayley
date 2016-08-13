// Copyright 2016 The Cayley Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package clog provides a logging interface for cayley packages.
package clog

import "log"

// Logger is the clog logging interface.
type Logger interface {
	Infof(format string, args ...interface{})
	Warningf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
}

var logger Logger = stdlog{}

// SetLogger set the clog logging implementation.
func SetLogger(l Logger) { logger = l }

var verbosity int

// V returns whether the current clog verbosity is above the specified level.
func V(level int) bool { return verbosity >= level }

// SetV sets the clog verbosity level.
func SetV(level int) { verbosity = level }

// Infof logs information level messages.
func Infof(format string, args ...interface{}) {
	if logger != nil {
		logger.Infof(format, args...)
	}
}

// Warningf logs warning level messages.
func Warningf(format string, args ...interface{}) {
	if logger != nil {
		logger.Warningf(format, args...)
	}
}

// Errorf logs error level messages.
func Errorf(format string, args ...interface{}) {
	if logger != nil {
		logger.Errorf(format, args...)
	}
}

// Fatalf logs fatal messages and terminates the program.
func Fatalf(format string, args ...interface{}) {
	if logger != nil {
		logger.Fatalf(format, args...)
	}
}

// stdlog wraps the standard library logger.
type stdlog struct{}

func (stdlog) Infof(format string, args ...interface{})    { log.Printf(format, args...) }
func (stdlog) Warningf(format string, args ...interface{}) { log.Printf("WARN: "+format, args...) }
func (stdlog) Errorf(format string, args ...interface{})   { log.Printf("ERROR: "+format, args...) }
func (stdlog) Fatalf(format string, args ...interface{})   { log.Fatalf("FATAL: "+format, args...) }
