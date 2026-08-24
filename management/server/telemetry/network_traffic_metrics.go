package telemetry

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// NetworkTrafficMetrics tracks the Management flow receive, store, query, and cleanup paths.
// Labels are fixed enums only; account, peer, user, address, and event identifiers are forbidden.
type NetworkTrafficMetrics struct {
	receive         metric.Int64Counter
	receiveDuration metric.Float64Histogram
	store           metric.Int64Counter
	storeDuration   metric.Float64Histogram
	query           metric.Int64Counter
	queryDuration   metric.Float64Histogram
	queryRows       metric.Int64Histogram
	cleanup         metric.Int64Counter
	cleanupDuration metric.Float64Histogram
	cleanupRows     metric.Int64Counter
}

func NewNetworkTrafficMetrics(meter metric.Meter) (*NetworkTrafficMetrics, error) {
	receive, err := meter.Int64Counter("management.flow.receive.counter", metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}
	receiveDuration, err := meter.Float64Histogram("management.flow.receive.duration", metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	store, err := meter.Int64Counter("management.flow.store.counter", metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}
	storeDuration, err := meter.Float64Histogram("management.flow.store.duration", metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	query, err := meter.Int64Counter("management.flow.query.counter", metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}
	queryDuration, err := meter.Float64Histogram("management.flow.query.duration", metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	queryRows, err := meter.Int64Histogram("management.flow.query.rows", metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}
	cleanup, err := meter.Int64Counter("management.flow.cleanup.counter", metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}
	cleanupDuration, err := meter.Float64Histogram("management.flow.cleanup.duration", metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	cleanupRows, err := meter.Int64Counter("management.flow.cleanup.rows.counter", metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}
	return &NetworkTrafficMetrics{
		receive: receive, receiveDuration: receiveDuration,
		store: store, storeDuration: storeDuration,
		query: query, queryDuration: queryDuration, queryRows: queryRows,
		cleanup: cleanup, cleanupDuration: cleanupDuration, cleanupRows: cleanupRows,
	}, nil
}

func (m *NetworkTrafficMetrics) RecordReceive(ctx context.Context, result, reason string, duration time.Duration) {
	if m == nil {
		return
	}
	attrs := metric.WithAttributes(attribute.String("result", result), attribute.String("reason", reason))
	m.receive.Add(ctx, 1, attrs)
	m.receiveDuration.Record(ctx, duration.Seconds(), attrs)
}

func (m *NetworkTrafficMetrics) RecordStore(ctx context.Context, result string, duration time.Duration) {
	if m == nil {
		return
	}
	attrs := metric.WithAttributes(attribute.String("result", result))
	m.store.Add(ctx, 1, attrs)
	m.storeDuration.Record(ctx, duration.Seconds(), attrs)
}

func (m *NetworkTrafficMetrics) RecordQuery(ctx context.Context, view, result string, rows int, duration time.Duration) {
	if m == nil {
		return
	}
	attrs := metric.WithAttributes(attribute.String("view", view), attribute.String("result", result))
	m.query.Add(ctx, 1, attrs)
	m.queryDuration.Record(ctx, duration.Seconds(), attrs)
	if rows >= 0 {
		m.queryRows.Record(ctx, int64(rows), metric.WithAttributes(attribute.String("view", view)))
	}
}

func (m *NetworkTrafficMetrics) RecordCleanup(ctx context.Context, result string, rows int64, duration time.Duration) {
	if m == nil {
		return
	}
	attrs := metric.WithAttributes(attribute.String("result", result))
	m.cleanup.Add(ctx, 1, attrs)
	m.cleanupDuration.Record(ctx, duration.Seconds(), attrs)
	if rows > 0 {
		m.cleanupRows.Add(ctx, rows, metric.WithAttributes(attribute.String("reason", "retention_or_limit")))
	}
}
