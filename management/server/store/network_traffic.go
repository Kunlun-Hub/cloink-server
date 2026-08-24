package store

import (
	"context"
	"errors"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/netbirdio/netbird/management/internals/modules/networktraffic"
	"github.com/netbirdio/netbird/management/server/types"
	"github.com/netbirdio/netbird/shared/management/status"
)

// CreateNetworkTrafficEvent stores one client-reported flow event. Replayed
// event IDs are treated as successful writes so the client can discard them.
func (s *SqlStore) CreateNetworkTrafficEvent(ctx context.Context, event *networktraffic.Event) error {
	if event == nil {
		return errors.New("network traffic event is nil")
	}

	result := s.db.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(event)
	if result.Error != nil {
		log.WithContext(ctx).Errorf("failed to create network traffic event: %v", result.Error)
		return status.Errorf(status.Internal, "failed to create network traffic event")
	}
	return nil
}

// GetNetworkTrafficPolicy resolves both peer ACL rule IDs and routed policy
// IDs emitted by the official client.
func (s *SqlStore) GetNetworkTrafficPolicy(ctx context.Context, lockStrength LockingStrength, accountID, ruleOrPolicyID string) (*types.Policy, error) {
	query := s.db.WithContext(ctx).Model(&types.Policy{}).
		Distinct("policies.*").
		Joins("LEFT JOIN policy_rules ON policy_rules.policy_id = policies.id").
		Where("policies.account_id = ? AND (policies.id = ? OR policy_rules.id = ?)", accountID, ruleOrPolicyID, ruleOrPolicyID)
	if lockStrength != LockingStrengthNone {
		query = query.Clauses(clause.Locking{Strength: string(lockStrength)})
	}

	var policy types.Policy
	if err := query.Take(&policy).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Errorf(status.NotFound, "network traffic policy not found")
		}
		return nil, status.Errorf(status.Internal, "resolve network traffic policy: %v", err)
	}
	return &policy, nil
}

// GetAccountNetworkTrafficEvents returns a paginated account-scoped event list.
func (s *SqlStore) GetAccountNetworkTrafficEvents(ctx context.Context, lockStrength LockingStrength, accountID string, filter networktraffic.Filter) ([]*networktraffic.Event, int64, error) {
	query := s.db.WithContext(ctx).Model(&networktraffic.Event{}).Where("account_id = ?", accountID)
	query = applyNetworkTrafficFilters(query, filter)

	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, status.Errorf(status.Internal, "count network traffic events: %v", err)
	}

	query = query.Order(filter.SortColumn() + " " + strings.ToUpper(filter.SortOrder())).
		Order("id " + strings.ToUpper(filter.SortOrder())).
		Limit(filter.PageSize).Offset(filter.Offset())
	if lockStrength != LockingStrengthNone {
		query = query.Clauses(clause.Locking{Strength: string(lockStrength)})
	}

	var events []*networktraffic.Event
	if err := query.Find(&events).Error; err != nil {
		return nil, 0, status.Errorf(status.Internal, "get network traffic events: %v", err)
	}
	return events, total, nil
}

// GetAccountNetworkTrafficGroups returns one page of account-scoped collection groups.
// Counter overflow is returned as an internal error rather than a truncated total.
func (s *SqlStore) GetAccountNetworkTrafficGroups(ctx context.Context, lockStrength LockingStrength, accountID string, filter networktraffic.Filter) ([]*networktraffic.Group, int64, error) {
	if filter.Page < 1 || filter.PageSize < 1 || filter.PageSize > networktraffic.MaxPageSize {
		return nil, 0, status.Errorf(status.InvalidArgument, "invalid network traffic group pagination")
	}
	if lockStrength != LockingStrengthNone {
		return nil, 0, status.Errorf(status.InvalidArgument, "network traffic group queries do not support row locking")
	}
	filtered := func() *gorm.DB {
		return applyNetworkTrafficFilters(s.db.WithContext(ctx).Model(&networktraffic.Event{}).Where("account_id = ?", accountID), filter)
	}
	grouped := filtered().Select(`window_start,
		user_id, MAX(user_name) AS user_name, MAX(user_email) AS user_email, reporter_id,
		COUNT(*) AS detail_count, COALESCE(SUM(rx_bytes), 0) AS rx_bytes, COALESCE(SUM(rx_packets), 0) AS rx_packets,
		COALESCE(SUM(tx_bytes), 0) AS tx_bytes, COALESCE(SUM(tx_packets), 0) AS tx_packets,
		COALESCE(SUM(num_of_starts), 0) AS num_of_starts, COALESCE(SUM(num_of_ends), 0) AS num_of_ends, COALESCE(SUM(num_of_drops), 0) AS num_of_drops`).
		Group("window_start, user_id, reporter_id")
	query := s.db.WithContext(ctx).Table("(?) AS network_traffic_groups", grouped).
		Select("network_traffic_groups.*, COUNT(*) OVER() AS total_groups").
		Order("window_start DESC, user_id DESC, reporter_id DESC").
		Limit(filter.PageSize).Offset(filter.Offset())
	var groups []*networktraffic.Group
	if err := query.Scan(&groups).Error; err != nil {
		return nil, 0, status.Errorf(status.Internal, "get network traffic groups: %v", err)
	}
	if len(groups) > 0 {
		return groups, groups[0].TotalGroups, nil
	}
	if filter.Offset() == 0 {
		return groups, 0, nil
	}
	var total int64
	if err := s.db.WithContext(ctx).Table("(?) AS network_traffic_groups", grouped).Count(&total).Error; err != nil {
		return nil, 0, status.Errorf(status.Internal, "count network traffic groups: %v", err)
	}
	return groups, total, nil
}

// GetAccountNetworkTrafficGroupEvents returns one page of an exact account-scoped group.
func (s *SqlStore) GetAccountNetworkTrafficGroupEvents(ctx context.Context, lockStrength LockingStrength, accountID string, filter networktraffic.Filter, windowStart time.Time, userID, reporterID string) ([]*networktraffic.Event, int64, error) {
	if filter.Page < 1 || filter.PageSize < 1 || filter.PageSize > networktraffic.MaxPageSize {
		return nil, 0, status.Errorf(status.InvalidArgument, "invalid network traffic detail pagination")
	}
	query := s.db.WithContext(ctx).Model(&networktraffic.Event{}).
		Where("account_id = ? AND window_start = ? AND user_id = ? AND reporter_id = ?", accountID, windowStart, userID, reporterID)
	query = applyNetworkTrafficFilters(query, filter)

	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, status.Errorf(status.Internal, "count network traffic group events: %v", err)
	}

	query = query.Order("timestamp DESC, id DESC").Limit(filter.PageSize).Offset(filter.Offset())
	if lockStrength != LockingStrengthNone {
		query = query.Clauses(clause.Locking{Strength: string(lockStrength)})
	}

	var events []*networktraffic.Event
	if err := query.Find(&events).Error; err != nil {
		return nil, 0, status.Errorf(status.Internal, "get network traffic group events: %v", err)
	}
	return events, total, nil
}

// CleanupNetworkTrafficEvents removes expired events and trims accounts to the
// configured maximum. A non-positive maxPerAccount disables the count limit.
func (s *SqlStore) CleanupNetworkTrafficEvents(ctx context.Context, olderThan time.Time, maxPerAccount int) (int64, error) {
	var deleted int64
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Where("timestamp < ?", olderThan).Delete(&networktraffic.Event{})
		if result.Error != nil {
			return result.Error
		}
		deleted = result.RowsAffected
		if maxPerAccount <= 0 {
			return nil
		}

		var accountIDs []string
		if err := tx.Model(&networktraffic.Event{}).Distinct("account_id").Pluck("account_id", &accountIDs).Error; err != nil {
			return err
		}
		for _, accountID := range accountIDs {
			var count int64
			if err := tx.Model(&networktraffic.Event{}).Where("account_id = ?", accountID).Count(&count).Error; err != nil {
				return err
			}
			if count <= int64(maxPerAccount) {
				continue
			}

			var excessIDs []string
			if err := tx.Model(&networktraffic.Event{}).
				Where("account_id = ?", accountID).
				Order("timestamp DESC, id DESC").Limit(int(count)-maxPerAccount).Offset(maxPerAccount).
				Pluck("id", &excessIDs).Error; err != nil {
				return err
			}
			if len(excessIDs) == 0 {
				continue
			}
			result = tx.Where("id IN ?", excessIDs).Delete(&networktraffic.Event{})
			if result.Error != nil {
				return result.Error
			}
			deleted += result.RowsAffected
		}
		return nil
	})
	if err != nil {
		return 0, status.Errorf(status.Internal, "cleanup network traffic events: %v", err)
	}
	return deleted, nil
}

func applyNetworkTrafficFilters(query *gorm.DB, filter networktraffic.Filter) *gorm.DB {
	if filter.Search != nil {
		pattern := "%" + strings.ToLower(*filter.Search) + "%"
		query = query.Where("LOWER(user_name) LIKE ? OR LOWER(user_email) LIKE ? OR LOWER(source_name) LIKE ? OR LOWER(destination_name) LIKE ? OR LOWER(source_address) LIKE ? OR LOWER(destination_address) LIKE ?", pattern, pattern, pattern, pattern, pattern, pattern)
	}
	if filter.UserID != nil {
		query = query.Where("user_id = ?", *filter.UserID)
	}
	if filter.ReporterID != nil {
		query = query.Where("reporter_id = ?", *filter.ReporterID)
	}
	if filter.SourceID != nil {
		query = query.Where("source_id = ?", *filter.SourceID)
	}
	if filter.DestinationID != nil {
		query = query.Where("destination_id = ?", *filter.DestinationID)
	}
	if filter.Protocol != nil {
		query = query.Where("protocol = ?", *filter.Protocol)
	}
	if filter.EventType != nil {
		query = query.Where("event_type = ?", *filter.EventType)
	}
	if filter.ConnectionType != nil {
		query = query.Where("connection_type = ?", *filter.ConnectionType)
	}
	if filter.Direction != nil {
		query = query.Where("direction = ?", *filter.Direction)
	}
	if filter.StartDate != nil {
		query = query.Where("timestamp >= ?", *filter.StartDate)
	}
	if filter.EndDate != nil {
		query = query.Where("timestamp <= ?", *filter.EndDate)
	}
	return query
}
