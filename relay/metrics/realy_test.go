package metrics

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func counterValue(t *testing.T, ctx context.Context, reader *sdkmetric.ManualReader, name string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok, "metric %s is not an int64 sum", name)
			var total int64
			for _, dp := range sum.DataPoints {
				total += dp.Value
			}
			return total
		}
	}
	return 0
}

func newTestMetrics(t *testing.T) (*Metrics, context.Context, *sdkmetric.ManualReader) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m, err := NewMetrics(ctx, provider.Meter("test"))
	require.NoError(t, err)
	return m, ctx, reader
}

func TestMetrics_ByteCountersFlush(t *testing.T) {
	m, ctx, reader := newTestMetrics(t)

	m.AddBytesSent(1500)
	m.AddBytesRecv(3000)

	// Existing metric names must keep working; the flush lands within the interval.
	require.Eventually(t, func() bool {
		return counterValue(t, ctx, reader, "relay_transfer_sent_bytes_total") == 1500 &&
			counterValue(t, ctx, reader, "relay_transfer_received_bytes_total") == 3000
	}, 5*time.Second, 50*time.Millisecond, "byte counters were not flushed to OTel")

	// A later flush carries only the new delta, no double counting.
	m.AddBytesSent(500)
	require.Eventually(t, func() bool {
		return counterValue(t, ctx, reader, "relay_transfer_sent_bytes_total") == 2000
	}, 5*time.Second, 50*time.Millisecond, "second flush did not carry only the new delta")
}

func TestMetrics_RecordDroppedTransportPacket(t *testing.T) {
	m, ctx, reader := newTestMetrics(t)

	m.RecordDroppedTransportPacket()
	m.RecordDroppedTransportPacket()

	// Dropped packets are accumulated atomically and flushed to OTel on the
	// interval, so the metric becomes visible after a flush.
	require.Eventually(t, func() bool {
		return counterValue(t, ctx, reader, "relay_transport_dropped_packets_total") == 2
	}, 5*time.Second, 50*time.Millisecond, "dropped packets were not flushed to OTel")

	// A later flush carries only the new delta, no double counting.
	m.RecordDroppedTransportPacket()
	require.Eventually(t, func() bool {
		return counterValue(t, ctx, reader, "relay_transport_dropped_packets_total") == 3
	}, 5*time.Second, 50*time.Millisecond, "second flush did not carry only the new delta")
}
