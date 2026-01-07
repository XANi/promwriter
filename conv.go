package promwriter

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
