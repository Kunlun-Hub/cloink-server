package events

import (
	"context"
	"time"

	"github.com/netbirdio/netbird/management/server/telemetry"
)

func recordNetworkTrafficQuery(metrics *telemetry.NetworkTrafficMetrics, ctx context.Context, view string, started time.Time, rows int, err error) {
	result := "success"
	if err != nil {
		result = "error"
	}
	metrics.RecordQuery(ctx, view, result, rows, time.Since(started))
}
