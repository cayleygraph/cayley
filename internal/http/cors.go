package http

import (
	"net/http"
)

// CORS adds CORS related headers to responses
func CORS(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if origin := req.Header.Get("Origin"); origin != "" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
			w.Header().Set("Access-Control-Allow-Headers",
				"Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
		}
		h.ServeHTTP(w, req)
	})
}

// HandlePreflight is an http.Handler for CORS Prelight requests
func HandlePreflight(w http.ResponseWriter, r *http.Request) {
	// Adjust status code to 204
	w.WriteHeader(http.StatusNoContent)
}
