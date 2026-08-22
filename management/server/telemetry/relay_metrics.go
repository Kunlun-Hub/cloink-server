package telemetry

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// RelayMetrics tracks low-cardinality Relay management operations.
type RelayMetrics struct {
	register      metric.Int64Counter
	configPush    metric.Int64Counter
	probe         metric.Int64Counter
	probeDuration metric.Float64Histogram
}

func NewRelayMetrics(meter metric.Meter) (*RelayMetrics, error) {
	register, err := meter.Int64Counter("management.relay.register.counter", metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}
	configPush, err := meter.Int64Counter("management.relay.config.push.counter", metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}
	probe, err := meter.Int64Counter("management.relay.probe.counter", metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}
	probeDuration, err := meter.Float64Histogram("management.relay.probe.duration", metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	return &RelayMetrics{register: register, configPush: configPush, probe: probe, probeDuration: probeDuration}, nil
}

func (m *RelayMetrics) CountRegister(ctx context.Context, result string) {
	if m != nil {
		m.register.Add(ctx, 1, metric.WithAttributes(attribute.String("result", result)))
	}
}

func (m *RelayMetrics) CountConfigPush(ctx context.Context, result string) {
	if m != nil {
		m.configPush.Add(ctx, 1, metric.WithAttributes(attribute.String("result", result)))
	}
}

func (m *RelayMetrics) RecordProbe(ctx context.Context, result string, duration time.Duration) {
	if m != nil {
		attrs := metric.WithAttributes(attribute.String("result", result))
		m.probe.Add(ctx, 1, attrs)
		m.probeDuration.Record(ctx, duration.Seconds(), attrs)
	}
}
