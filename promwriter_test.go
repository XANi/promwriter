package promwriter

import (
	"bytes"
	"github.com/XANi/goneric"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

		var wr prompb.WriteRequest
		if err := gproto.Unmarshal(dec, &wr); err != nil {
			t.Fatalf("proto unmarshal error: %v", err)
			return
		}

		select {
		case recv <- &wr:
		default:
		}

		w.WriteHeader(204)
	}))
	defer srv.Close()

	logger := zap.NewExample().Sugar()

	// test defaults
	_, err := New(Config{
		InstanceName: srv.URL,
		Logger:       logger,
	})
	assert.NoError(t, err)

	cfg := Config{
		URL:              srv.URL,
		MaxBatchDuration: time.Millisecond * 1,
		MaxBatchLength:   1,
		Logger:           logger,
	}
	pw, err := New(cfg)
	if err != nil {
		t.Fatalf("creating PromWriter: %v", err)
	}

	// Create a sample metric
	now := time.Now().Truncate(time.Millisecond) // keep milliseconds deterministic
	ev := PrometheusWrite{
		Name:    "test_metric",
		Labels:  map[string]string{"host": "unit-test", "zone": "us-east-1"},
		Value:   42.5,
		Counter: true, // exercise counter path (label/name handling isn't used server-side here)
		TS:      now,
	}

	if err := pw.WriteMetric(ev); err != nil {
		t.Fatalf("WriteMetric returned error: %v", err)
	}
	var b bytes.Buffer
	_, err = ev.Write(&b)
	require.NoError(t, err)
	assert.Contains(t, b.String(), `test_metric_total{host="unit-test",zone="us-east-1"} 42.500000`)

	select {
	case got := <-recv:
		// basic assertions: one timeseries with one sample
		if len(got.Timeseries) != 1 {
			t.Fatalf("expected 1 timeseries, got %d", len(got.Timeseries))
		}
		ts := got.Timeseries[0]
		assert.Len(t, got.Timeseries, 1)

		labelMap := goneric.SliceMapFunc(func(t prompb.Label) (string, string) {
			return t.Name, t.Value
		}, ts.Labels)
		// add the __name__ to labels for tests to match
		ev.Labels["__name__"] = "test_metric_total"
		assert.Equal(t, ev.Labels, labelMap)

		assert.Len(t, ts.Samples, 1)
		s := ts.Samples[0]
		assert.Equal(t, ev.Value, s.Value)
		assert.Equal(t, ev.TS.UnixMilli(), s.Timestamp)
	// Wait for the server to receive a request
	case <-time.After(time.Second * 2):
		t.Fatalf("timed out waiting for remote write")
	}
}
