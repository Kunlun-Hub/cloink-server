package networktraffic

import (
	"fmt"
	"math"
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
	MaxQueryValueLength = 1024
)

var validSortFields = map[string]string{
	"timestamp":   "timestamp",
	"protocol":    "protocol",
	"direction":   "direction",
	"type":        "event_type",
	"user_id":     "user_id",
	"reporter_id": "reporter_id",
}

var validSortOrders = map[string]string{"asc": "", "desc": ""}

// Filter contains validated network traffic query parameters.
type Filter struct {
	Page     int
	PageSize int
	SortBy   string
	SortOrd  string

	Search             *string
	UserID             *string
	ReporterID         *string
	SourceID           *string
	SourceKey          *string
	DestinationID      *string
	DestinationAddress *string
	Protocol           *int
	EventType          *string
	ConnectionType     *string
	Direction          *string
	StartDate          *time.Time
	EndDate            *time.Time
}

// ParseFromRequest parses and bounds query parameters from an HTTP request.
func (f *Filter) ParseFromRequest(r *http.Request) error {
	query := r.URL.Query()
	var err error
	if f.Page, err = positiveInt(query, "page", 1, math.MaxInt); err != nil {
		return err
	}
	if f.PageSize, err = positiveInt(query, "page_size", DefaultPageSize, MaxPageSize); err != nil {
		return err
	}
	if f.Page > math.MaxInt/f.PageSize {
		return fmt.Errorf("network traffic pagination is too large")
	}
	if f.SortBy, err = enumValue(query, "sort_by", DefaultSortBy, validSortFields, false); err != nil {
		return err
	}
	if f.SortOrd, err = enumValue(query, "sort_order", DefaultSortOrder, validSortOrders, true); err != nil {
		return err
	}
	f.SortOrd = strings.ToLower(f.SortOrd)
	if f.Search, err = optionalQueryString(query, "search", true); err != nil {
		return err
	}
	if f.UserID, err = optionalQueryString(query, "user_id", false); err != nil {
		return err
	}
	if f.ReporterID, err = optionalQueryString(query, "reporter_id", false); err != nil {
		return err
	}
	if f.SourceID, err = optionalQueryString(query, "source_id", false); err != nil {
		return err
	}
	if f.SourceKey, err = optionalQueryString(query, "source_key", false); err != nil {
		return err
	}
	if f.DestinationID, err = optionalQueryString(query, "destination_id", false); err != nil {
		return err
	}
	if f.DestinationAddress, err = optionalQueryString(query, "destination_address", false); err != nil {
		return err
	}
	if f.Protocol, err = optionalProtocol(query); err != nil {
		return err
	}
	if f.EventType, err = optionalEnum(query, "type", "TYPE_UNKNOWN", "TYPE_START", "TYPE_END", "TYPE_DROP"); err != nil {
		return err
	}
	if f.ConnectionType, err = optionalEnum(query, "connection_type", ConnectionTypeP2P, ConnectionTypeRouted); err != nil {
		return err
	}
	if f.Direction, err = optionalEnum(query, "direction", "DIRECTION_UNKNOWN", "INGRESS", "EGRESS"); err != nil {
		return err
	}
	if f.StartDate, err = optionalRFC3339(query, "start_date"); err != nil {
		return err
	}
	if f.EndDate, err = optionalRFC3339(query, "end_date"); err != nil {
		return err
	}
	return f.normalizeDateRange(time.Now().UTC())
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

func (f *Filter) normalizeDateRange(now time.Time) error {
	endDate := now
	if f.EndDate != nil {
		endDate = f.EndDate.UTC()
	}
	startDate := endDate.Add(-DefaultRangeMinutes * time.Minute)
	if f.StartDate != nil {
		startDate = f.StartDate.UTC()
	}
	if startDate.After(endDate) {
		return fmt.Errorf("start_date must not be after end_date")
	}
	if startDate.Before(endDate.Add(-MaxDateRangeDays * 24 * time.Hour)) {
		return fmt.Errorf("network traffic date range exceeds %d days", MaxDateRangeDays)
	}
	f.StartDate, f.EndDate = &startDate, &endDate
	return nil
}

func scalar(query map[string][]string, name string) (string, bool, error) {
	values, ok := query[name]
	if !ok {
		return "", false, nil
	}
	if len(values) != 1 {
		return "", false, fmt.Errorf("%s must be specified once", name)
	}
	return values[0], true, nil
}

func positiveInt(query map[string][]string, name string, fallback, maximum int) (int, error) {
	value, present, err := scalar(query, name)
	if err != nil || !present {
		return fallback, err
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 1 || parsed > maximum {
		return 0, fmt.Errorf("invalid %s", name)
	}
	return parsed, nil
}

func enumValue(query map[string][]string, name, fallback string, valid map[string]string, foldCase bool) (string, error) {
	value, present, err := scalar(query, name)
	if err != nil || !present {
		return fallback, err
	}
	if foldCase {
		value = strings.ToLower(value)
	}
	if _, ok := valid[value]; !ok {
		return "", fmt.Errorf("invalid %s", name)
	}
	return value, nil
}

func optionalQueryString(query map[string][]string, name string, trim bool) (*string, error) {
	value, present, err := scalar(query, name)
	if err != nil || !present {
		return nil, err
	}
	if trim {
		value = strings.TrimSpace(value)
	}
	if value == "" {
		return nil, fmt.Errorf("%s must not be empty", name)
	}
	if len(value) > MaxQueryValueLength {
		return nil, fmt.Errorf("%s is too long", name)
	}
	return &value, nil
}

func optionalProtocol(query map[string][]string) (*int, error) {
	value, present, err := scalar(query, "protocol")
	if err != nil || !present {
		return nil, err
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 || parsed > 255 {
		return nil, fmt.Errorf("invalid protocol")
	}
	return &parsed, nil
}

func optionalEnum(query map[string][]string, name string, valid ...string) (*string, error) {
	value, present, err := scalar(query, name)
	if err != nil || !present {
		return nil, err
	}
	for _, candidate := range valid {
		if value == candidate {
			return &value, nil
		}
	}
	return nil, fmt.Errorf("invalid %s", name)
}

func optionalRFC3339(query map[string][]string, name string) (*time.Time, error) {
	value, present, err := scalar(query, name)
	if err != nil || !present {
		return nil, err
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return nil, fmt.Errorf("invalid %s", name)
	}
	parsed = parsed.UTC()
	return &parsed, nil
}
