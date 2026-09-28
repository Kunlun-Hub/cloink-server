package relays

import (
	"context"
	"slices"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/netbirdio/netbird/management/server/store"
)

// RemoveGroupFromRelays drops groupID from the distribution groups of every
// relay of the account, both in the in-memory active registry and in the
// persisted registrations. It is called after a group is deleted; group
// deletion is infrequent, so the registry is scanned linearly without an
// index. A relay whose group list becomes empty reverts to global
// distribution, per the FilterRelayDescriptorsForGroups contract. It returns
// true when at least one relay referenced the group.
//
// The cleanup lives in this package because the active registry does. The
// account manager owns group deletion and calls this after the delete
// transaction commits, following the existing pattern of core code calling
// into this package (e.g. management/internals/shared/grpc/conversion.go).
func RemoveGroupFromRelays(ctx context.Context, storeManager store.Store, accountID, groupID string) bool {
	if accountID == "" || groupID == "" {
		return false
	}
	removedActive := activeRelayRegistry.removeGroup(accountID, groupID)
	removedStored := removeGroupFromStoredRelays(ctx, storeManager, accountID, groupID)
	return removedActive || removedStored
}

// removeGroup drops groupID from the distribution groups of every relay of
// the account, returning true when at least one relay referenced it.
func (r *relayRegistry) removeGroup(accountID, groupID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	prefix := registryKey(accountID, "")
	removed := false
	for key, relay := range r.relays {
		if !strings.HasPrefix(key, prefix) || !slices.Contains(relay.Groups, groupID) {
			continue
		}
		relay.Groups = slices.DeleteFunc(slices.Clone(relay.Groups), func(id string) bool { return id == groupID })
		if len(relay.Groups) == 0 {
			relay.Groups = nil
		}
		r.relays[key] = relay
		removed = true
	}
	return removed
}

// removeGroupFromStoredRelays drops groupID from every persisted relay
// registration of the account, mirroring the updateStoredRelayGroups pattern.
func removeGroupFromStoredRelays(ctx context.Context, storeManager store.Store, accountID, groupID string) bool {
	if storeManager == nil {
		return false
	}

	removed := false
	if err := storeManager.ExecuteInTransaction(ctx, func(transaction store.Store) error {
		settings, err := transaction.GetAccountSettings(ctx, store.LockingStrengthUpdate, accountID)
		if err != nil {
			return err
		}
		if settings == nil || settings.Extra == nil || len(settings.Extra.RegisteredRelays) == 0 {
			return nil
		}

		settings = settings.Copy()
		for key, relay := range settings.Extra.RegisteredRelays {
			if !slices.Contains(relay.Groups, groupID) {
				continue
			}
			relay.Groups = slices.DeleteFunc(slices.Clone(relay.Groups), func(id string) bool { return id == groupID })
			if len(relay.Groups) == 0 {
				relay.Groups = nil
			}
			settings.Extra.RegisteredRelays[key] = relay
			removed = true
		}
		if !removed {
			return nil
		}
		return transaction.SaveAccountSettings(ctx, accountID, settings)
	}); err != nil {
		log.WithContext(ctx).Warnf("failed to remove group %s from stored relays for account %s: %v", groupID, accountID, err)
	}
	return removed
}
