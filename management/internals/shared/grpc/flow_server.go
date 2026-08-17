package grpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/netip"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/netbirdio/netbird/flow/proto"
	"github.com/netbirdio/netbird/management/internals/modules/networktraffic"
	"github.com/netbirdio/netbird/management/server/account"
	nbpeer "github.com/netbirdio/netbird/management/server/peer"
	"github.com/netbirdio/netbird/management/server/store"
	internalStatus "github.com/netbirdio/netbird/shared/management/status"
)

const (
	maxFlowAddressPort = math.MaxUint16
	maxFlowFutureSkew  = 5 * time.Minute
	maxFlowIDLength    = 1024
)

type permanentFlowError struct {
	err error
}

func (e *permanentFlowError) Error() string {
	return e.err.Error()
}

func (e *permanentFlowError) Unwrap() error {
	return e.err
}

func permanentFlowErrorf(format string, args ...any) error {
	return &permanentFlowError{err: fmt.Errorf(format, args...)}
}

// FlowServer receives the official v0.77 flow protocol and persists events.
type FlowServer struct {
	proto.UnimplementedFlowServiceServer
	accountManager account.Manager
	configManager  *networktraffic.ConfigManager
	cleanupMu      sync.Mutex
	cleanupCancel  context.CancelFunc
	cleanupDone    chan struct{}
}

// NewFlowServer creates a self-hosted flow receiver.
func NewFlowServer(accountManager account.Manager) *FlowServer {
	return &FlowServer{accountManager: accountManager}
}

// SetConfigManager sets the token validator used by the receiver.
func (s *FlowServer) SetConfigManager(configManager *networktraffic.ConfigManager) {
	s.configManager = configManager
}

// StartPeriodicCleanup removes expired and excess flow events until ctx ends.
func (s *FlowServer) StartPeriodicCleanup(ctx context.Context, retention time.Duration, maxPerAccount int, interval time.Duration) {
	s.cleanupMu.Lock()
	defer s.cleanupMu.Unlock()
	if s.cleanupCancel != nil {
		return
	}
	if retention <= 0 {
		retention = 48 * time.Hour
	}
	if interval <= 0 {
		interval = time.Hour
	}
	cleanupCtx, cancel := context.WithCancel(ctx)
	s.cleanupCancel = cancel
	s.cleanupDone = make(chan struct{})
	done := s.cleanupDone
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		s.cleanup(cleanupCtx, retention, maxPerAccount)
		for {
			select {
			case <-cleanupCtx.Done():
				return
			case <-ticker.C:
				s.cleanup(cleanupCtx, retention, maxPerAccount)
			}
		}
	}()
}

// StopCleanup stops the flow event cleanup worker and waits for it to exit.
func (s *FlowServer) StopCleanup() {
	s.cleanupMu.Lock()
	cancel, done := s.cleanupCancel, s.cleanupDone
	s.cleanupCancel = nil
	s.cleanupDone = nil
	s.cleanupMu.Unlock()
	if cancel != nil {
		cancel()
		<-done
	}
}

func (s *FlowServer) cleanup(ctx context.Context, retention time.Duration, maxPerAccount int) {
	if _, err := s.accountManager.GetStore().CleanupNetworkTrafficEvents(ctx, time.Now().UTC().Add(-retention), maxPerAccount); err != nil && ctx.Err() == nil {
		log.WithContext(ctx).Warnf("failed to clean up network traffic events: %v", err)
	}
}

// Events receives flow events and acknowledges only events that are durable or
// permanently invalid. Database failures leave events unacknowledged so agents retry.
func (s *FlowServer) Events(stream proto.FlowService_EventsServer) error {
	claims, err := s.authenticate(stream.Context())
	if err != nil {
		return err
	}

	initiator, err := stream.Recv()
	if err != nil {
		return err
	}
	if !initiator.GetIsInitiator() || initiator.GetFlowFields() != nil || len(initiator.GetEventId()) != 0 {
		return status.Error(codes.InvalidArgument, "invalid flow initiator frame")
	}
	if err := stream.Send(&proto.FlowEventAck{IsInitiator: true}); err != nil {
		return err
	}

	for {
		event, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}

		saveErr := s.saveEvent(stream.Context(), claims, event)
		if saveErr != nil {
			var permanentErr *permanentFlowError
			if !errors.As(saveErr, &permanentErr) {
				return saveErr
			}
			log.WithContext(stream.Context()).Debugf("discarding invalid network traffic event: %v", permanentErr)
			if _, err := uuid.FromBytes(event.GetEventId()); err != nil {
				return status.Error(codes.InvalidArgument, "invalid flow event ID")
			}
		}

		if err := stream.Send(&proto.FlowEventAck{EventId: event.GetEventId()}); err != nil {
			return err
		}
	}
}

func (s *FlowServer) authenticate(ctx context.Context) (networktraffic.TokenClaims, error) {
	if s.configManager == nil {
		return networktraffic.TokenClaims{}, status.Error(codes.Unavailable, "flow receiver is not configured")
	}

	values := metadata.ValueFromIncomingContext(ctx, "authorization")
	if len(values) != 1 {
		return networktraffic.TokenClaims{}, status.Error(codes.Unauthenticated, "flow authorization is required")
	}
	value := strings.TrimSpace(values[0])
	if len(value) < len("Bearer ") || !strings.EqualFold(value[:len("Bearer ")], "Bearer ") {
		return networktraffic.TokenClaims{}, status.Error(codes.Unauthenticated, "invalid flow authorization")
	}
	parts := strings.Split(strings.TrimSpace(value[len("Bearer "):]), ".")
	if len(parts) != 2 {
		return networktraffic.TokenClaims{}, status.Error(codes.Unauthenticated, "invalid flow authorization")
	}
	claims, err := s.configManager.Validate(parts[1], parts[0])
	if err != nil {
		return networktraffic.TokenClaims{}, status.Error(codes.Unauthenticated, "invalid flow authorization")
	}
	return claims, nil
}

func (s *FlowServer) saveEvent(ctx context.Context, claims networktraffic.TokenClaims, event *proto.FlowEvent) error {
	if event == nil {
		return permanentFlowErrorf("flow event is empty")
	}
	if event.GetIsInitiator() {
		return permanentFlowErrorf("unexpected flow initiator frame")
	}
	if err := validateFlowEvent(event); err != nil {
		return permanentFlowErrorf("invalid flow event: %v", err)
	}

	reporter, err := s.accountManager.GetStore().GetPeerByPeerPubKey(ctx, store.LockingStrengthNone, publicKeyString(event.GetPublicKey()))
	if err != nil {
		if isInternalStoreError(err) {
			return fmt.Errorf("resolve flow reporter: %w", err)
		}
		return permanentFlowErrorf("resolve flow reporter: %v", err)
	}
	if reporter.AccountID != claims.AccountID || reporter.ID != claims.PeerID || reporter.Key != publicKeyString(event.GetPublicKey()) {
		return permanentFlowErrorf("flow token and reporter do not match")
	}

	settings, err := s.accountManager.GetStore().GetAccountSettings(ctx, store.LockingStrengthNone, claims.AccountID)
	if err != nil {
		return fmt.Errorf("resolve flow settings: %w", err)
	}
	groupIDs, err := s.accountManager.GetStore().GetPeerGroupIDs(ctx, store.LockingStrengthNone, claims.AccountID, claims.PeerID)
	if err != nil {
		return fmt.Errorf("resolve flow groups: %w", err)
	}
	if settings == nil || !networktraffic.FlowEnabledForPeer(settings.Extra, groupIDs) {
		return permanentFlowErrorf("flow reporting is disabled for peer")
	}

	fields := event.GetFlowFields()
	sourcePort, destinationPort, icmpType, icmpCode := flowConnectionValues(fields)
	source, err := s.resolveEndpoint(ctx, claims.AccountID, fields.GetSourceIp(), sourcePort, fields.GetSourceResourceId())
	if err != nil {
		return err
	}
	destination, err := s.resolveEndpoint(ctx, claims.AccountID, fields.GetDestIp(), destinationPort, fields.GetDestResourceId())
	if err != nil {
		return err
	}

	userName, userEmail := flowUser(ctx, s.accountManager.GetStore(), reporter)
	policyID, policyName, err := s.resolvePolicy(ctx, claims.AccountID, fields.GetRuleId())
	if err != nil {
		return err
	}
	eventID, _ := uuid.FromBytes(event.GetEventId())
	flowID, _ := uuid.FromBytes(fields.GetFlowId())
	timestamp := time.Now().UTC()
	if value := event.GetTimestamp(); value != nil {
		timestamp = value.AsTime().UTC()
	}
	windowStart := timestamp
	if value := event.GetWindowStart(); value != nil {
		windowStart = value.AsTime().UTC()
	}
	windowEnd := timestamp
	if value := event.GetWindowEnd(); value != nil {
		windowEnd = value.AsTime().UTC()
	}

	record := &networktraffic.Event{
		ID:                     eventID.String(),
		AccountID:              claims.AccountID,
		FlowID:                 flowID.String(),
		Timestamp:              timestamp,
		WindowStart:            windowStart,
		WindowEnd:              windowEnd,
		EventType:              fields.GetType().String(),
		Direction:              fields.GetDirection().String(),
		Protocol:               int(fields.GetProtocol()),
		ConnectionType:         connectionType(source, destination),
		ReporterID:             reporter.ID,
		UserID:                 reporter.UserID,
		UserName:               userName,
		UserEmail:              userEmail,
		SourceID:               source.ID,
		SourceType:             source.Type,
		SourceName:             source.Name,
		SourceAddress:          source.Address,
		SourceDNSLabel:         source.DNSLabel,
		SourceOS:               source.OS,
		SourceCountryCode:      source.CountryCode,
		SourceCityName:         source.CityName,
		DestinationID:          destination.ID,
		DestinationType:        destination.Type,
		DestinationName:        destination.Name,
		DestinationAddress:     destination.Address,
		DestinationDNSLabel:    destination.DNSLabel,
		DestinationOS:          destination.OS,
		DestinationCountryCode: destination.CountryCode,
		DestinationCityName:    destination.CityName,
		PolicyID:               policyID,
		PolicyName:             policyName,
		ICMPType:               icmpType,
		ICMPCode:               icmpCode,
		RxBytes:                int64(fields.GetRxBytes()),
		RxPackets:              int64(fields.GetRxPackets()),
		TxBytes:                int64(fields.GetTxBytes()),
		TxPackets:              int64(fields.GetTxPackets()),
		NumOfStarts:            int(fields.GetNumOfStarts()),
		NumOfEnds:              int(fields.GetNumOfEnds()),
		NumOfDrops:             int(fields.GetNumOfDrops()),
	}
	return s.accountManager.GetStore().CreateNetworkTrafficEvent(ctx, record)
}

func validateFlowEvent(event *proto.FlowEvent) error {
	if _, err := uuid.FromBytes(event.GetEventId()); err != nil {
		return fmt.Errorf("event id: %w", err)
	}
	fields := event.GetFlowFields()
	if fields == nil {
		return errors.New("flow fields are empty")
	}
	if _, err := uuid.FromBytes(fields.GetFlowId()); err != nil {
		return fmt.Errorf("flow id: %w", err)
	}
	if len(event.GetPublicKey()) != wgtypes.KeyLen {
		return fmt.Errorf("public key length %d", len(event.GetPublicKey()))
	}
	now := time.Now().UTC()
	timestamp := event.GetTimestamp()
	if timestamp == nil {
		return errors.New("timestamp is required")
	}
	if err := timestamp.CheckValid(); err != nil {
		return fmt.Errorf("timestamp: %w", err)
	}
	if timestamp.AsTime().After(now.Add(maxFlowFutureSkew)) {
		return errors.New("timestamp is too far in the future")
	}
	windowStart := event.GetWindowStart()
	if windowStart != nil {
		if err := windowStart.CheckValid(); err != nil {
			return fmt.Errorf("window start: %w", err)
		}
		if windowStart.AsTime().After(now.Add(maxFlowFutureSkew)) {
			return errors.New("window start is too far in the future")
		}
	}
	windowEnd := event.GetWindowEnd()
	if windowEnd != nil {
		if err := windowEnd.CheckValid(); err != nil {
			return fmt.Errorf("window end: %w", err)
		}
		if windowEnd.AsTime().After(now.Add(maxFlowFutureSkew)) {
			return errors.New("window end is too far in the future")
		}
	}
	if windowStart != nil && windowEnd != nil && windowStart.AsTime().After(windowEnd.AsTime()) {
		return errors.New("window start is after window end")
	}
	if windowStart != nil && timestamp.AsTime().Before(windowStart.AsTime()) {
		return errors.New("timestamp is before window start")
	}
	if windowEnd != nil && timestamp.AsTime().After(windowEnd.AsTime()) {
		return errors.New("timestamp is after window end")
	}
	if fields.GetType() < proto.Type_TYPE_UNKNOWN || fields.GetType() > proto.Type_TYPE_DROP {
		return fmt.Errorf("flow type %d is out of range", fields.GetType())
	}
	if fields.GetDirection() < proto.Direction_DIRECTION_UNKNOWN || fields.GetDirection() > proto.Direction_EGRESS {
		return fmt.Errorf("flow direction %d is out of range", fields.GetDirection())
	}
	if fields.GetProtocol() > 255 {
		return fmt.Errorf("protocol %d is out of range", fields.GetProtocol())
	}
	if len(fields.GetSourceIp()) == 0 || len(fields.GetDestIp()) == 0 {
		return errors.New("source and destination IP are required")
	}
	if len(fields.GetRuleId()) > maxFlowIDLength || len(fields.GetSourceResourceId()) > maxFlowIDLength || len(fields.GetDestResourceId()) > maxFlowIDLength {
		return errors.New("flow identifier is too long")
	}
	if _, ok := netip.AddrFromSlice(fields.GetSourceIp()); !ok {
		return errors.New("invalid source IP")
	}
	if _, ok := netip.AddrFromSlice(fields.GetDestIp()); !ok {
		return errors.New("invalid destination IP")
	}
	for name, value := range map[string]uint64{
		"rx_bytes": fields.GetRxBytes(), "rx_packets": fields.GetRxPackets(),
		"tx_bytes": fields.GetTxBytes(), "tx_packets": fields.GetTxPackets(),
		"num_of_starts": fields.GetNumOfStarts(), "num_of_ends": fields.GetNumOfEnds(),
		"num_of_drops": fields.GetNumOfDrops(),
	} {
		if value > math.MaxInt64 {
			return networktraffic.InvalidCounterError(name, value)
		}
	}
	portInfo := fields.GetPortInfo()
	icmpInfo := fields.GetIcmpInfo()
	if portInfo == nil && icmpInfo == nil {
		return errors.New("flow connection information is required")
	}
	if fields.GetProtocol() == 1 || fields.GetProtocol() == 58 {
		if icmpInfo == nil {
			return errors.New("ICMP flow requires ICMP information")
		}
	} else if portInfo == nil {
		return errors.New("non-ICMP flow requires port information")
	}
	if portInfo != nil {
		if portInfo.GetSourcePort() > maxFlowAddressPort || portInfo.GetDestPort() > maxFlowAddressPort {
			return errors.New("flow port is out of range")
		}
	}
	if icmpInfo != nil {
		if icmpInfo.GetIcmpType() > 255 || icmpInfo.GetIcmpCode() > 255 {
			return errors.New("ICMP value is out of range")
		}
	}
	return nil
}

func (s *FlowServer) resolvePolicy(ctx context.Context, accountID string, ruleID []byte) (string, string, error) {
	if len(ruleID) == 0 {
		return "", "", nil
	}
	policy, err := s.accountManager.GetStore().GetNetworkTrafficPolicy(ctx, store.LockingStrengthNone, accountID, string(ruleID))
	if err != nil {
		if isStoreErrorType(err, internalStatus.NotFound) {
			return "", "", nil
		}
		return "", "", fmt.Errorf("resolve flow policy: %w", err)
	}
	if policy == nil {
		return "", "", nil
	}
	return policy.ID, policy.Name, nil
}

func publicKeyString(value []byte) string {
	key, err := wgtypes.NewKey(value)
	if err != nil {
		return ""
	}
	return key.String()
}

func flowConnectionValues(fields *proto.FlowFields) (sourcePort, destinationPort, icmpType, icmpCode int) {
	if ports := fields.GetPortInfo(); ports != nil {
		sourcePort = int(ports.GetSourcePort())
		destinationPort = int(ports.GetDestPort())
	}
	if icmp := fields.GetIcmpInfo(); icmp != nil {
		icmpType = int(icmp.GetIcmpType())
		icmpCode = int(icmp.GetIcmpCode())
	}
	return
}

type resolvedFlowEndpoint struct {
	ID, Type, Name, Address, DNSLabel, OS, CountryCode, CityName string
}

func (s *FlowServer) resolveEndpoint(ctx context.Context, accountID string, rawIP []byte, port int, resourceID []byte) (*resolvedFlowEndpoint, error) {
	addr, ok := netip.AddrFromSlice(rawIP)
	if !ok {
		return nil, permanentFlowErrorf("invalid endpoint IP")
	}
	addr = addr.Unmap()
	address := networktraffic.FormatAddress(addr, uint16(port))
	dbStore := s.accountManager.GetStore()
	if len(resourceID) > 0 {
		resource, err := dbStore.GetNetworkResourceByID(ctx, store.LockingStrengthNone, accountID, string(resourceID))
		if err == nil && resource != nil {
			return &resolvedFlowEndpoint{ID: resource.ID, Type: networktraffic.EndpointTypeHostResource, Name: resource.Name, Address: address}, nil
		}
		if isInternalStoreError(err) {
			return nil, fmt.Errorf("resolve flow resource: %w", err)
		}
	}
	peer, err := dbStore.GetPeerByIP(ctx, store.LockingStrengthNone, accountID, net.IP(addr.AsSlice()))
	if err == nil && peer != nil {
		return peerFlowEndpoint(peer, address), nil
	}
	if isInternalStoreError(err) {
		return nil, fmt.Errorf("resolve flow peer: %w", err)
	}
	return &resolvedFlowEndpoint{Type: networktraffic.EndpointTypeUnknown, Name: addr.String(), Address: address}, nil
}

func peerFlowEndpoint(peer *nbpeer.Peer, address string) *resolvedFlowEndpoint {
	return &resolvedFlowEndpoint{
		ID: peer.ID, Type: networktraffic.EndpointTypePeer, Name: peer.Name, Address: address,
		DNSLabel: peer.DNSLabel, OS: peer.Meta.OS, CountryCode: peer.Location.CountryCode, CityName: peer.Location.CityName,
	}
}

func flowUser(ctx context.Context, dbStore store.Store, reporter *nbpeer.Peer) (string, string) {
	if reporter.UserID == "" {
		return "", ""
	}
	user, err := dbStore.GetUserByUserID(ctx, store.LockingStrengthNone, reporter.UserID)
	if err != nil || user == nil {
		return "", ""
	}
	return user.Name, user.Email
}

func isInternalStoreError(err error) bool {
	return isStoreErrorType(err, internalStatus.Internal)
}

func isStoreErrorType(err error, errorType internalStatus.Type) bool {
	if err == nil {
		return false
	}
	storeStatus, ok := internalStatus.FromError(err)
	return ok && storeStatus.Type() == errorType
}

func connectionType(source, destination *resolvedFlowEndpoint) string {
	if source.Type == networktraffic.EndpointTypePeer && destination.Type == networktraffic.EndpointTypePeer {
		return networktraffic.ConnectionTypeP2P
	}
	return networktraffic.ConnectionTypeRouted
}

var _ proto.FlowServiceServer = (*FlowServer)(nil)
