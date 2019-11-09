package http

import "net/http"

// HandleHealth is a route for handling health checks to the server
func HandleHealth(w http.ResponseWriter, r *http.Request) {
	// Adjust status code to 204
	w.WriteHeader(http.StatusNoContent)
}
