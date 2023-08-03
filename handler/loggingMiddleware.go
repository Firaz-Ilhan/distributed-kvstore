package handler

import (
	"log"
	"net/http"
	"time"
)

func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startTime := time.Now()
		statusWriter := &statusResponseWriter{ResponseWriter: w}

		defer func() {
			log.Printf(
				"Request: %s %s, Response: %d, Duration: %vms, User-Agent: %s, Response size: %d bytes",
				r.Method,
				r.URL,
				statusWriter.status,
				time.Since(startTime).Milliseconds(),
				r.UserAgent(),
				statusWriter.size,
			)
		}()

		next.ServeHTTP(statusWriter, r)
	})
}

type statusResponseWriter struct {
	http.ResponseWriter
	status int
	size   int
}

func (w *statusResponseWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusResponseWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	size, err := w.ResponseWriter.Write(b)
	w.size += size
	return size, err
}
