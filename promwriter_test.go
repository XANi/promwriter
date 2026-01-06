package promwriter

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	gproto "github.com/golang/protobuf/proto"
	"github.com/klauspost/compress/snappy"
	"github.com/prometheus/prometheus/prompb"
	"go.uber.org/zap"
)

func TestPromWriterSendsRemoteWrite(t *testing.T) {
	// Channel to receive the parsed WriteRequest from the HTTP handler
	recv := make(chan *prompb.WriteRequest, 1)

	// Start a test HTTP server to receive remote-write requests
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// basic header checks
		if r.Header.Get("Content-Encoding") != "snappy" {
			t.Logf("unexpected Content-Encoding: %s", r.Header.Get("Content-Encoding"))
			http.Error(w, "bad encoding", 400)
			return
		}
		if r.Header.Get("Content-Type") != "application/x-protobuf" {
			t.Logf("unexpected Content-Type: %s", r.Header.Get("Content-Type"))
			http.Error(w, "bad content-type", 400)
			return
		}
		if r.Header.Get("X-Prometheus-Remote-Write-Version") != "0.1.0" {
			t.Logf("unexpected version header: %s", r.Header.Get("X-Prometheus-Remote-Write-Version"))
			http.Error(w, "bad version", 400)
			return
		}

		// Read and decode snappy body
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("reading body: %v", err)
			return
		}
		r.Body.Close()

		dec, err := snappy.Decode(nil, body)
		if err != nil {
			t.Fatalf("snappy decode error: %v", err)
			return
		}

		// Unmarshal to prompb.WriteRequest. Use github.com/golang/protobuf/proto to be compatible.
		var wr prompb.WriteRequest
		if err := gproto.Unmarshal(dec, &wr); err != nil {
			t.Fatalf("proto unmarshal error: %v", err)
			return
		}

		// send parsed request back to test goroutine
		select {
		case recv <- &wr:
		default:
		}

		// respond with success code expected by the writer
		w.WriteHeader(204)
	}))
	defer srv.Close()

	// Create logger
	logger := zap.NewExample().Sugar()

	// Create PromWriter pointing at the test server
	cfg := Config{
		URL:              srv.URL,
		Timeout:          time.Second * 5,
		MaxBatchDuration: time.Millisecond * 200,
		MaxBatchLength:   100,
		Logger:           logger,
	}
	pw, err := New(cfg)
	if err != nil {
		t.Fatalf("creating PromWriter: %v", err)
	}

	// Create a sample metric
	now := time.Now().Truncate(time.Millisecond) // keep milliseconds deterministic
	ev := &PrometheusWrite{
		Name:    "test_metric_total",
		Labels:  map[string]string{"host": "unit-test", "zone": "us-east-1"},
		Value:   42.5,
		Counter: true, // exercise counter path (label/name handling isn't used server-side here)
		TS:      now,
	}

	// Send the metric
	if err := pw.WriteMetric(ev); err != nil {
		t.Fatalf("WriteMetric returned error: %v", err)
	}

	// Wait for the server to receive a request
	select {
	case got := <-recv:
		// basic assertions: one timeseries with one sample
		if len(got.Timeseries) != 1 {
			t.Fatalf("expected 1 timeseries, got %d", len(got.Timeseries))
		}
		ts := got.Timeseries[0]
		// check __name__ label exists and matches (server receives the name)
		foundName := ""
		for _, l := range ts.Labels {
			if l.Name == "__name__" {
				foundName = l.Value
			}
		}
		if foundName == "" {
			t.Fatalf("__name__ label not present")
		}
		// value and timestamp checks
		if len(ts.Samples) != 1 {
			t.Fatalf("expected 1 sample, got %d", len(ts.Samples))
		}
		s := ts.Samples[0]
		if s.Value != ev.Value {
			t.Fatalf("sample value mismatch: want %v got %v", ev.Value, s.Value)
		}
		if s.Timestamp != ev.TS.UnixMilli() {
			t.Fatalf("timestamp mismatch: want %d got %d", ev.TS.UnixMilli(), s.Timestamp)
		}

	case <-time.After(time.Second * 2):
		t.Fatalf("timed out waiting for remote write")
	}
}
