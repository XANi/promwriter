# Promwriter

Simple lib for writing Prometheus data

```go

	cfg := Config{
		URL:              srv.URL,
		//MaxBatchDuration: time.Second * 3,
		//MaxBatchLength:   1000,
		//Logger:           zapSugaredLogger, // or 
	}
    writer, err := promwriter.New(cfg)
    metric := promwriter.Metric{
		Name:    "test_metric",
		Labels:  map[string]string{"host": "unit-test", "zone": "us-east-1"},
		Value:   42.5,
		Counter: true, // adds "_total" to metric name
		TS:      Time.Now(),
	}
    err := writer.WriteMetric(metric)
```

will error out if queue is full, WriteMetric waits up to MaxBatchDuration for timeout

Alternatively you can use text representation

```go
    metric := promwriter.Metric{
		Name:    "test_metric",
		Labels:  map[string]string{"host": "unit-test", "zone": "us-east-1"},
		Value:   42.5,
		Counter: true, // adds "_total" to metric name
		TS:      Time.Now(),
	}
    n,err := metric.Write(ioWriter)
```
