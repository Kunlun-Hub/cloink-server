package telemetry

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/metric/noop"
)

func TestRelayMetrics(t *testing.T) {
	metrics, err := NewRelayMetrics(noop.NewMeterProvider().Meter("test"))
	if err != nil {
		t.Fatal(err)
	}
	metrics.CountRegister(context.Background(), "success")
	metrics.CountConfigPush(context.Background(), "skipped")
	metrics.RecordProbe(context.Background(), "error", time.Second)
}
