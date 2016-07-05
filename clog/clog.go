package clog

import "log"

type Logger interface {
	Infof(format string, args ...interface{})
	Warningf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
}

var defaultLogger Logger = logger{}

func SetLogger(l Logger) { defaultLogger = l }

var verbosity int

func V(v int) bool { return v <= verbosity }
func SetV(v int)   { verbosity = v }

func Infof(format string, args ...interface{})    { defaultLogger.Infof(format, args...) }
func Warningf(format string, args ...interface{}) { defaultLogger.Warningf(format, args...) }
func Errorf(format string, args ...interface{})   { defaultLogger.Errorf(format, args...) }
func Fatalf(format string, args ...interface{})   { defaultLogger.Fatalf(format, args...) }

type logger struct{}

func (logger) Infof(format string, args ...interface{})    { log.Printf(format, args...) }
func (logger) Warningf(format string, args ...interface{}) { log.Printf("WARN: "+format, args...) }
func (logger) Errorf(format string, args ...interface{})   { log.Printf("ERROR: "+format, args...) }
func (logger) Fatalf(format string, args ...interface{})   { log.Fatalf("FATAL: "+format, args...) }
