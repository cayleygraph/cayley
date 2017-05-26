package glog

import (
	"fmt"

	"github.com/cayleygraph/cayley/clog"
	"github.com/golang/glog"
)

func init() {
	clog.SetLogger(Logger{})
}

type Logger struct{}

func (Logger) Infof(format string, args ...interface{}) {
	glog.InfoDepth(3, fmt.Sprintf(format, args...))
}
func (Logger) Warningf(format string, args ...interface{}) {
	glog.WarningDepth(3, fmt.Sprintf(format, args...))
}
func (Logger) Errorf(format string, args ...interface{}) {
	glog.ErrorDepth(3, fmt.Sprintf(format, args...))
}
func (Logger) Fatalf(format string, args ...interface{}) {
	glog.FatalDepth(3, fmt.Sprintf(format, args...))
}

func (Logger) V(level int) bool {
	return bool(glog.V(glog.Level(level)))
}

func (Logger) SetV(v int) {
	glog.Warningf("changing log level is not supported; run command with '-v %d' flag", v)
}
