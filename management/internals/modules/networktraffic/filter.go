package networktraffic

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultPageSize     = 1000
	MaxPageSize         = 50000
	DefaultSortBy       = "timestamp"
	DefaultSortOrder    = "desc"
	DefaultRangeMinutes = 5
	MaxDateRangeDays    = 15
)

var validSortFields = map[string]string{
	"timestamp":   "timestamp",
	"protocol":    "protocol",
	"direction":   "direction",
	"type":        "event_type",
	"user_id":     "user_id",
	"reporter_id": "reporter_id",
}

// Filter contains validated network traffic query parameters.
type Filter struct {
	Page     int
	PageSize int
	SortBy   string
	SortOrd  string

	Search         *string
	UserID         *string
	ReporterID     *string
	SourceID       *string
	DestinationID  *string
	Protocol       *int
	EventType      *string
	ConnectionType *string
	Direction      *string
	StartDate      *time.Time
	EndDate        *time.Time
}

// ParseFromRequest parses and bounds query parameters from an HTTP request.
func (f *Filter) ParseFromRequest(r *http.Request) {
	query := r.URL.Query()
	f.Page = parsePositiveInt(query.Get("page"), 1)
	f.PageSize = min(parsePositiveInt(query.Get("page_size"), DefaultPageSize), MaxPageSize)
	f.SortBy = parseSortField(query.Get("sort_by"))
	f.SortOrd = parseSortOrder(query.Get("sort_order"))
	f.Search = parseOptionalString(query.Get("search"))
	f.UserID = parseOptionalString(query.Get("user_id"))
	f.ReporterID = parseOptionalString(query.Get("reporter_id"))
	f.SourceID = parseOptionalString(query.Get("source_id"))
	f.DestinationID = parseOptionalString(query.Get("destination_id"))
	f.Protocol = parseOptionalInt(query.Get("protocol"))
	f.EventType = parseOptionalString(query.Get("type"))
	f.ConnectionType = parseOptionalString(query.Get("connection_type"))
	f.Direction = parseOptionalString(query.Get("direction"))
	f.StartDate = parseOptionalRFC3339(query.Get("start_date"))
	f.EndDate = parseOptionalRFC3339(query.Get("end_date"))
	f.normalizeDateRange(time.Now().UTC())
}

// Offset returns the database result offset.
func (f Filter) Offset() int {
	return (f.Page - 1) * f.PageSize
}

// SortColumn returns a whitelisted database sort column.
func (f Filter) SortColumn() string {
	if field, ok := validSortFields[f.SortBy]; ok {
		return field
	}
	return validSortFields[DefaultSortBy]
}

// SortOrder returns a normalized database sort order.
func (f Filter) SortOrder() string {
	if f.SortOrd == "asc" || f.SortOrd == "desc" {
		return f.SortOrd
	}
	return DefaultSortOrder
}

func (f *Filter) normalizeDateRange(now time.Time) {
	endDate := now
	if f.EndDate != nil {
		endDate = f.EndDate.UTC()
	}

	startDate := endDate.Add(-DefaultRangeMinutes * time.Minute)
	if f.StartDate != nil {
		startDate = f.StartDate.UTC()
	}
	if startDate.After(endDate) {
		startDate = endDate
	}

	maxStartDate := endDate.Add(-MaxDateRangeDays * 24 * time.Hour)
	if startDate.Before(maxStartDate) {
		startDate = maxStartDate
	}

	f.StartDate = &startDate
	f.EndDate = &endDate
}

func parsePositiveInt(value string, fallback int) int {
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func parseOptionalString(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return &value
}

func parseOptionalInt(value string) *int {
	if value == "" {
		return nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return nil
	}
	return &parsed
}

func parseOptionalRFC3339(value string) *time.Time {
	if value == "" {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return nil
	}
	return &parsed
}

func parseSortField(value string) string {
	if _, ok := validSortFields[value]; ok {
		return value
	}
	return DefaultSortBy
}

func parseSortOrder(value string) string {
	value = strings.ToLower(value)
	if value == "asc" || value == "desc" {
		return value
	}
	return DefaultSortOrder
}
