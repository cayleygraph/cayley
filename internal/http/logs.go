package http

import (
	"net/http"
	"time"

	"github.com/cayleygraph/cayley/clog"
)

// statusWriter wraps http.ResponseWriter and captures the written status code
type statusWriter struct {
	http.ResponseWriter
	code int
}

// newStatusWriter returns an initialized statusWriter
func newStatusWriter(w http.ResponseWriter) *statusWriter {
	return &statusWriter{w, 200}
}

// WriteHeader wraps ResponseWriter WriteHeader and saves the code
func (w *statusWriter) WriteHeader(code int) {
	w.ResponseWriter.WriteHeader(code)
	w.code = code
}

// getAddress returns the address of the incoming request
func getAddress(req *http.Request) string {
	addr := req.Header.Get("X-Real-IP")
	if addr == "" {
		addr = req.Header.Get("X-Forwarded-For")
		if addr == "" {
			addr = req.RemoteAddr
		}
	}
	return addr
}

// LogRequest wraps a http.Handler and emits logs about the request and the response
func LogRequest(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		start := time.Now()
		addr := getAddress(req)
		sw := newStatusWriter(w)
		clog.Infof("started %s %s for %s", req.Method, req.URL.Path, addr)
		handler.ServeHTTP(sw, req)
		clog.Infof("completed %v %s %s in %v", sw.code, http.StatusText(sw.code), req.URL.Path, time.Since(start))
	})
}
