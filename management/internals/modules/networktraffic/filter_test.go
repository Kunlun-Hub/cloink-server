package networktraffic

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFilterParseFromRequestRejectsInvalidQueries(t *testing.T) {
	tests := []string{
		"page=0",
		"page=1&page=2",
		"page_size=50001",
		"protocol=256",
		"protocol=x",
		"direction=invalid",
		"start_date=invalid",
		"start_date=2026-08-22T01%3A00%3A00Z&end_date=2026-08-22T00%3A00%3A00Z",
		"start_date=2026-08-01T00%3A00%3A00Z&end_date=2026-08-22T00%3A00%3A00Z",
		"search=",
		"user_id=",
		"reporter_id=" + strings.Repeat("a", MaxQueryValueLength+1),
	}
	for _, query := range tests {
		t.Run(query, func(t *testing.T) {
			var filter Filter
			err := filter.ParseFromRequest(httptest.NewRequest("GET", "/?"+query, nil))
			require.Error(t, err)
		})
	}
}

func TestFilterParseFromRequestNormalizesValidQuery(t *testing.T) {
	var filter Filter
	err := filter.ParseFromRequest(httptest.NewRequest("GET", "/?page=2&page_size=5&sort_by=protocol&sort_order=ASC&protocol=0&start_date=2026-08-22T08%3A00%3A00%2B08%3A00&end_date=2026-08-22T00%3A05%3A00Z", nil))
	require.NoError(t, err)
	require.Equal(t, 2, filter.Page)
	require.Equal(t, 5, filter.PageSize)
	require.Equal(t, "protocol", filter.SortBy)
	require.Equal(t, "asc", filter.SortOrd)
	require.Equal(t, 0, *filter.Protocol)
	require.Equal(t, time.Date(2026, 8, 22, 0, 0, 0, 0, time.UTC), *filter.StartDate)
	require.Equal(t, time.Date(2026, 8, 22, 0, 5, 0, 0, time.UTC), *filter.EndDate)
}
