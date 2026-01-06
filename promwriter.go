package promwriter

import (
	"bytes"
	"fmt"
	"github.com/klauspost/compress/snappy"
	"github.com/prometheus/prometheus/prompb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"

	//"github.com/golang/protobuf/proto"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

type Config struct {
	InstanceName     string `yaml:"-"`
	URL              string
	Timeout          time.Duration      `yaml:"timeout"`
	MaxBatchDuration time.Duration      `yaml:"max_batch_duration"`
	MaxBatchLength   int                `yaml:"max_batch_length"`
	HostLabelFile    string             `yaml:"host_label_file"`
	Logger           *zap.SugaredLogger `yaml:"-"`
}

type PromWriter struct {
	cfg          Config
	l            *zap.SugaredLogger
	writeChannel chan PrometheusWrite
	http         *http.Client
}
type PrometheusWrite struct {
	Name    string
	Labels  map[string]string
	Value   float64
	Counter bool
	TS      time.Time
}

func (p *PrometheusWrite) Write(f io.Writer) (n int, err error) {
	buf := bytes.Buffer{}
	if p.Counter {
		buf.WriteString(promquotelabel("total_" + p.Name))
	} else {
		buf.WriteString(promquotelabel(p.Name))
	}
	if len(p.Labels) > 0 {
		tagList := []string{}
		for k, v := range p.Labels {
			tagList = append(tagList, promquotelabel(k)+"="+promquoteval(v))
		}
		sort.Strings(tagList)
		buf.WriteString("{" + strings.Join(tagList, ",") + "}")
	}
	buf.WriteString(fmt.Sprintf(" %f %d\n", p.Value, p.TS.UnixMilli()))
	return f.Write(buf.Bytes())
}
func promquoteval(s string) string {
	return fmt.Sprintf("%q", s)
}

var labelReplacer = strings.NewReplacer(
	" ", "_",
	"-", "_",
	"^", "_",
	"#", "_",
)

func promquotelabel(s string) string {
	return labelReplacer.Replace(s)
}

func New(cfg Config) (*PromWriter, error) {
	cfg.Logger.Infof("cfg: %+v", cfg)
	if cfg.Timeout == 0 {
		cfg.Timeout = time.Second * 10
	}
	if cfg.MaxBatchDuration <= 0 {
		cfg.MaxBatchDuration = time.Second * 3
	}
	if cfg.MaxBatchLength <= 0 {
		cfg.MaxBatchLength = 1000
	}
	w := PromWriter{
		cfg: cfg,
		l:   cfg.Logger,
		http: &http.Client{
			Transport:     nil,
			CheckRedirect: nil,
			Jar:           nil,
			Timeout:       cfg.Timeout,
		},
		writeChannel: make(chan PrometheusWrite, cfg.MaxBatchLength*5),
	}
	url, err := url.Parse(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("error parsing prometheus URL: %w", err)
	} else {
		// cut down url so any basic auth pass won't show in logs
		cfg.Logger.Infof("starting prometheus writer to %s%s", url.Host, url.Path)
	}
	go w.writer()
	return &w, nil
}

func (p *PromWriter) WriteChannel() chan PrometheusWrite {
	return p.writeChannel
}

func (p *PromWriter) WriteMetric(m *PrometheusWrite) error {
	select {
	case p.writeChannel <- *m:
		return nil
	}

}

//func (p *PromWriter) WriteCollectd(c datatypes.CollectdHTTP) {
//	prom := datatypes.PrometheusWrite{
//		TS: time.UnixMilli(int64(c.Time * 1000)),
//		Labels: map[string]string{
//			"host": c.Host,
//		},
//	}
//	switch c.Type {
//	case "gauge":
//		prom.Name = c.Plugin
//	case "counter":
//		prom.Name = c.Plugin + "_total"
//	case "derive":
//		prom.Name = c.Plugin + "_total"
//	case prom.Name: // if type has same name as plugin, dont repeat
//	default:
//		prom.Name = c.Plugin + "_" + strings.TrimLeft(strings.TrimPrefix(c.Type, c.Plugin), "_-")
//		prom.Name = strings.Trim(prom.Name, "-_")
//	}
//	if len(c.PluginInstance) > 0 {
//		prom.Labels["instance"] = c.PluginInstance
//	}
//	if len(c.TypeInstance) > 0 {
//		prom.Labels["type_instance"] = c.TypeInstance
//	}
//	maxWriteDelay := time.After(time.Millisecond * 100)
//	if len(c.Values) == 1 {
//		prom.Value = c.Values[0]
//		select {
//		case p.writeChannel <- prom:
//		case <-maxWriteDelay:
//			p.l.Warnf("queue delay exceeded, q size %d", len(p.writeChannel))
//		}
//	} else {
//		for idx, v := range c.Values {
//			promEv := prom
//			promEv.Labels = map[string]string{}
//			for k, v := range prom.Labels {
//				promEv.Labels[k] = v
//			}
//			promEv.Labels["type"] = c.Dsnames[idx]
//			switch c.Dstypes[idx] {
//			case "derive", "counter":
//				promEv.Type = datatypes.MetricTypeCounter
//			case "gauge":
//				promEv.Type = datatypes.MetricTypeGauge
//			}
//			promEv.Value = v
//			select {
//			case p.writeChannel <- promEv:
//			case <-maxWriteDelay:
//				p.l.Warnf("queue delay exceeded")
//			}
//		}
//	}
//
//}

func (p *PromWriter) writer() {
	timeBackoff := time.Second
	backoffTriggered := false
	for {
		events := []PrometheusWrite{}
		deadline := time.After(p.cfg.MaxBatchDuration)
		var prWr PrometheusWrite
	tmout:
		for len(events) < p.cfg.MaxBatchLength {
			select {
			case prWr = <-p.writeChannel:
				events = append(events, prWr)
			case <-deadline:
				break tmout
			}
		}
		if len(events) > 0 {
			wr := &prompb.WriteRequest{
				Timeseries: make([]prompb.TimeSeries, 0),
			}

			for _, e := range events {
				dp := prompb.TimeSeries{
					Labels: []prompb.Label{{
						Name:  "__name__",
						Value: e.Name,
					}},
				}
				for k, v := range e.Labels {
					dp.Labels = append(dp.Labels, prompb.Label{
						Name:  k,
						Value: v,
					})
				}
				// protocol requires them to be sorted, just in case some server is stupid enough to enforce this silliness
				sort.Slice(dp.Labels, func(i, j int) bool {
					return dp.Labels[i].Name < dp.Labels[j].Name
				})
				dp.Samples = []prompb.Sample{{
					Timestamp: e.TS.UnixMilli(),
					Value:     e.Value,
				}}

				wr.Timeseries = append(wr.Timeseries, dp)
				//	wr.Metadata = append(wr.Metadata, prompb.MetricMetadata{
				//		Type:             0,
				//		MetricFamilyName: "",
				//		Help:             "",
				//		Unit:             "",
				//	})
			}
			//p.monEvCount.Update(float64(len(events)))
			b, err := proto.Marshal(protoadapt.MessageV2Of(wr))
			buf := snappy.Encode(nil, b)
			req, err := http.NewRequest("POST", p.cfg.URL, bytes.NewBuffer(buf))

			req.Header.Set("Content-Encoding", "snappy")
			req.Header.Set("Content-Type", "application/x-protobuf")
			req.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")

			resp, err := p.http.Do(req)
			if err != nil {
				p.l.Errorf("error sending request to %s: %s", p.cfg.URL, err)
				// TODO retry
				//p.monReqFailCount.Update(1)
				continue
			}

			if resp.StatusCode == 429 {
				backoffTriggered = true
				p.l.Infof("too many requests, sleeping for %s", timeBackoff)
				if timeBackoff < time.Minute {
					timeBackoff += (timeBackoff / 2)
				}
			} else {
				if timeBackoff > time.Minute {
					timeBackoff -= time.Second
				} else if timeBackoff < time.Second {
					timeBackoff = time.Second
				} else {
					timeBackoff -= time.Millisecond
				}

			}
			if backoffTriggered == true {
				time.Sleep(timeBackoff)
			}
			if resp.StatusCode != 204 && resp.StatusCode != 200 {
				body, _ := io.ReadAll(resp.Body)
				p.l.Errorf("!240 status: [%d]%s: %s", resp.StatusCode, resp.Status, string(body))
				//p.monReqFailCount.Update(1)
			} else {
				//p.monBatchSize.Update(float64(len(events)))
				//p.monReqOkCount.Update(1)
			}
			resp.Body.Close()
		}

	}
}
