package http

import (
	"net/http"
	"time"

	"github.com/cayleygraph/cayley/clog"
)

func LogRequest(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		start := time.Now()
		addr := req.Header.Get("X-Real-IP")
		if addr == "" {
			addr = req.Header.Get("X-Forwarded-For")
			if addr == "" {
				addr = req.RemoteAddr
			}
		}
		code := 200
		rw := &statusWriter{ResponseWriter: w, code: &code}
		clog.Infof("started %s %s for %s", req.Method, req.URL.Path, addr)
		handler.ServeHTTP(rw, req)
		clog.Infof("completed %v %s %s in %v", code, http.StatusText(code), req.URL.Path, time.Since(start))
	})
}
