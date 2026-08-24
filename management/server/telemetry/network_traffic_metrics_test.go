package telemetry

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/metric/noop"
)

func TestNetworkTrafficMetrics(t *testing.T) {
	metrics, err := NewNetworkTrafficMetrics(noop.NewMeterProvider().Meter("test"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	metrics.RecordReceive(ctx, "success", "none", time.Millisecond)
	metrics.RecordStore(ctx, "success", time.Millisecond)
	metrics.RecordQuery(ctx, "grouped", "success", 2, time.Millisecond)
	metrics.RecordCleanup(ctx, "success", 3, time.Millisecond)

	var unset *NetworkTrafficMetrics
	unset.RecordReceive(ctx, "error", "internal", 0)
	unset.RecordStore(ctx, "error", 0)
	unset.RecordQuery(ctx, "raw", "error", -1, 0)
	unset.RecordCleanup(ctx, "error", 0, 0)
}
