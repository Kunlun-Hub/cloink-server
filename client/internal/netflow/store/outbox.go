package store

import (
	"fmt"
	"io"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	nftypes "github.com/netbirdio/netbird/client/internal/netflow/types"
	flowproto "github.com/netbirdio/netbird/flow/proto"
)

const (
	outboxDirName       = "flow-outbox"
	outboxFileExt       = ".pb"
	maxOutboxAge        = 7 * 24 * time.Hour
	maxOutboxFiles      = 4096
	maxOutboxQuarantine = 16
	maxOutboxEntries    = 2 * maxOutboxFiles
)

func NewOutboxStore(stateDir string, publicKey []byte, maxEvents, maxBytes int) (*BoundedMemory, error) {
	if len(publicKey) == 0 {
		return nil, fmt.Errorf("public key is required for flow outbox")
	}
	dir := filepath.Join(stateDir, outboxDirName, fmt.Sprintf("%x", publicKey))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create outbox: %w", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return nil, fmt.Errorf("protect outbox: %w", err)
	}
	if err := syncDir(filepath.Dir(dir)); err != nil {
		return nil, fmt.Errorf("sync outbox parent: %w", err)
	}

	store := NewBoundedMemoryStore(maxEvents, maxBytes)
	store.persist = func(event *nftypes.Event) (int, error) {
		return persistEvent(dir, event)
	}
	store.remove = func(id uuid.UUID) error {
		err := os.Remove(filepath.Join(dir, id.String()+outboxFileExt))
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		return syncDir(dir)
	}
	if err := recoverEvents(store, dir); err != nil {
		return nil, err
	}
	return store, nil
}

func recoverEvents(store *BoundedMemory, dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open outbox: %w", err)
	}
	entries, readErr := d.ReadDir(maxOutboxEntries + 1)
	closeErr := d.Close()
	if readErr != nil && readErr != io.EOF {
		return fmt.Errorf("read outbox: %w", readErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close outbox: %w", closeErr)
	}
	if len(entries) > maxOutboxEntries {
		return fmt.Errorf("outbox contains more than %d entries", maxOutboxEntries)
	}

	live := make([]os.DirEntry, 0, min(len(entries), maxOutboxFiles))
	quarantined := 0
	for _, entry := range entries {
		name := entry.Name()
		path := filepath.Join(dir, name)
		if entry.IsDir() {
			return fmt.Errorf("outbox entry %q is a directory", name)
		}
		switch {
		case strings.HasPrefix(name, ".tmp-"):
			if err := os.Remove(path); err != nil {
				return fmt.Errorf("remove stale outbox temp file: %w", err)
			}
		case strings.HasSuffix(name, ".corrupt") || strings.Contains(name, ".corrupt."):
			if quarantined < maxOutboxQuarantine {
				quarantined++
			} else if err := os.Remove(path); err != nil {
				return fmt.Errorf("remove excess outbox quarantine: %w", err)
			}
		case strings.HasSuffix(name, outboxFileExt):
			live = append(live, entry)
			if len(live) > maxOutboxFiles {
				return fmt.Errorf("outbox contains more than %d live events", maxOutboxFiles)
			}
		default:
			return fmt.Errorf("unexpected outbox entry %q", name)
		}
	}

	cutoff := time.Now().Add(-maxOutboxAge)
	for _, entry := range live {
		path := filepath.Join(dir, entry.Name())
		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("inspect outbox event: %w", err)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("outbox entry %q is not a regular file", entry.Name())
		}
		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("open outbox event: %w", err)
		}
		data, readErr := io.ReadAll(io.LimitReader(file, int64(store.maxBytes)+1))
		closeErr := file.Close()
		if readErr != nil {
			return fmt.Errorf("read outbox event: %w", readErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close outbox event: %w", closeErr)
		}
		if len(data) > store.maxBytes {
			store.MarkIncomplete(IncompleteOutboxCorrupt)
			if err := quarantine(path, &quarantined); err != nil {
				return err
			}
			continue
		}
		event, err := decodeEvent(data)
		if err != nil || event.Timestamp.Before(cutoff) {
			store.MarkIncomplete(IncompleteOutboxCorrupt)
			if err := quarantine(path, &quarantined); err != nil {
				return err
			}
			continue
		}
		if filepath.Base(path) != event.ID.String()+outboxFileExt {
			store.MarkIncomplete(IncompleteOutboxCorrupt)
			if err := quarantine(path, &quarantined); err != nil {
				return err
			}
			continue
		}
		if len(store.events) >= store.maxEvents || store.bytes+len(data) > store.maxBytes {
			return fmt.Errorf("recovered outbox exceeds capacity")
		}
		store.events[event.ID] = event
		store.sizes[event.ID] = len(data)
		store.bytes += len(data)
	}
	return syncDir(dir)
}

func persistEvent(dir string, event *nftypes.Event) (size int, retErr error) {
	wire := eventToProto(event)
	if err := validateOutboxEvent(wire); err != nil {
		return 0, err
	}
	data, err := proto.Marshal(wire)
	if err != nil {
		return 0, fmt.Errorf("encode outbox event: %w", err)
	}
	target := filepath.Join(dir, event.ID.String()+outboxFileExt)
	if info, err := os.Lstat(target); err == nil {
		if !info.Mode().IsRegular() {
			return 0, fmt.Errorf("outbox target is not a regular file")
		}
		return int(info.Size()), nil
	} else if !os.IsNotExist(err) {
		return 0, fmt.Errorf("inspect outbox event: %w", err)
	}

	temp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return 0, fmt.Errorf("create outbox event: %w", err)
	}
	tempName := temp.Name()
	defer func() {
		_ = temp.Close()
		if retErr != nil {
			_ = os.Remove(tempName)
		}
	}()
	if _, err := temp.Write(data); err != nil {
		return 0, fmt.Errorf("write outbox event: %w", err)
	}
	if err := temp.Sync(); err != nil {
		return 0, fmt.Errorf("sync outbox event: %w", err)
	}
	if err := temp.Close(); err != nil {
		return 0, fmt.Errorf("close outbox event: %w", err)
	}
	if err := os.Rename(tempName, target); err != nil {
		return 0, fmt.Errorf("commit outbox event: %w", err)
	}
	if err := os.Chmod(target, 0o600); err != nil {
		return 0, fmt.Errorf("protect outbox event: %w", err)
	}
	if err := syncDir(dir); err != nil {
		return 0, fmt.Errorf("sync outbox: %w", err)
	}
	return len(data), nil
}

func quarantine(path string, count *int) error {
	if *count >= maxOutboxQuarantine {
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("remove corrupt outbox event: %w", err)
		}
		return syncDir(filepath.Dir(path))
	}

	target := path + ".corrupt"
	if _, err := os.Stat(target); err == nil {
		target += "." + uuid.NewString()
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("inspect quarantine target: %w", err)
	}
	if err := os.Rename(path, target); err != nil {
		return fmt.Errorf("quarantine outbox event: %w", err)
	}
	*count++
	return syncDir(filepath.Dir(path))
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func eventToProto(event *nftypes.Event) *flowproto.FlowEvent {
	fields := &flowproto.FlowFields{
		FlowId:           event.FlowID[:],
		RuleId:           event.RuleID,
		Type:             flowproto.Type(event.Type),
		Direction:        flowproto.Direction(event.Direction),
		Protocol:         uint32(event.Protocol),
		SourceIp:         event.SourceIP.AsSlice(),
		DestIp:           event.DestIP.AsSlice(),
		RxPackets:        event.RxPackets,
		TxPackets:        event.TxPackets,
		RxBytes:          event.RxBytes,
		TxBytes:          event.TxBytes,
		SourceResourceId: event.SourceResourceID,
		DestResourceId:   event.DestResourceID,
		NumOfStarts:      event.NumOfStarts,
		NumOfEnds:        event.NumOfEnds,
		NumOfDrops:       event.NumOfDrops,
	}
	if event.Protocol == nftypes.ICMP || event.Protocol == nftypes.ICMPv6 {
		fields.ConnectionInfo = &flowproto.FlowFields_IcmpInfo{IcmpInfo: &flowproto.ICMPInfo{
			IcmpType: uint32(event.ICMPType), IcmpCode: uint32(event.ICMPCode),
		}}
	} else {
		fields.ConnectionInfo = &flowproto.FlowFields_PortInfo{PortInfo: &flowproto.PortInfo{
			SourcePort: uint32(event.SourcePort), DestPort: uint32(event.DestPort),
		}}
	}
	return &flowproto.FlowEvent{
		EventId: event.ID[:], Timestamp: timestamp(event.Timestamp),
		WindowStart: timestamp(event.WindowStart), WindowEnd: timestamp(event.WindowEnd),
		FlowFields: fields,
	}
}

func validateOutboxEvent(wire *flowproto.FlowEvent) error {
	if _, err := uuid.FromBytes(wire.GetEventId()); err != nil {
		return fmt.Errorf("event id: %w", err)
	}
	fields := wire.GetFlowFields()
	if fields == nil {
		return fmt.Errorf("flow fields are empty")
	}
	if _, err := uuid.FromBytes(fields.GetFlowId()); err != nil {
		return fmt.Errorf("flow id: %w", err)
	}
	if _, ok := netip.AddrFromSlice(fields.GetSourceIp()); !ok {
		return fmt.Errorf("invalid source IP")
	}
	if _, ok := netip.AddrFromSlice(fields.GetDestIp()); !ok {
		return fmt.Errorf("invalid destination IP")
	}
	if fields.GetProtocol() > 255 || fields.GetType() > flowproto.Type_TYPE_DROP || fields.GetDirection() > flowproto.Direction_EGRESS {
		return fmt.Errorf("flow enum is out of range")
	}
	if len(fields.GetRuleId()) > 1024 || len(fields.GetSourceResourceId()) > 1024 || len(fields.GetDestResourceId()) > 1024 {
		return fmt.Errorf("flow identifier is too long")
	}
	if ports := fields.GetPortInfo(); ports != nil && (ports.GetSourcePort() > 65535 || ports.GetDestPort() > 65535) {
		return fmt.Errorf("flow port is out of range")
	}
	if icmp := fields.GetIcmpInfo(); icmp != nil && (icmp.GetIcmpType() > 255 || icmp.GetIcmpCode() > 255) {
		return fmt.Errorf("ICMP value is out of range")
	}
	if wire.GetTimestamp() == nil || wire.GetWindowStart() == nil || wire.GetWindowEnd() == nil {
		return fmt.Errorf("missing event timestamp")
	}
	for _, timestamp := range []*timestamppb.Timestamp{wire.GetTimestamp(), wire.GetWindowStart(), wire.GetWindowEnd()} {
		if err := timestamp.CheckValid(); err != nil {
			return err
		}
	}
	if wire.GetWindowStart().AsTime().After(wire.GetWindowEnd().AsTime()) {
		return fmt.Errorf("window start is after window end")
	}
	return nil
}

func decodeEvent(data []byte) (*nftypes.Event, error) {
	wire := &flowproto.FlowEvent{}
	if err := proto.Unmarshal(data, wire); err != nil {
		return nil, err
	}
	if err := validateOutboxEvent(wire); err != nil {
		return nil, err
	}
	id, _ := uuid.FromBytes(wire.GetEventId())
	fields := wire.GetFlowFields()
	flowID, _ := uuid.FromBytes(fields.GetFlowId())
	src, _ := netip.AddrFromSlice(fields.GetSourceIp())
	dst, _ := netip.AddrFromSlice(fields.GetDestIp())
	event := &nftypes.Event{
		ID: id, Timestamp: wire.GetTimestamp().AsTime(), WindowStart: wire.GetWindowStart().AsTime(), WindowEnd: wire.GetWindowEnd().AsTime(),
		EventFields: nftypes.EventFields{
			FlowID: flowID, Type: nftypes.Type(fields.GetType()), RuleID: fields.GetRuleId(), Direction: nftypes.Direction(fields.GetDirection()),
			Protocol: nftypes.Protocol(fields.GetProtocol()), SourceIP: src.Unmap(), DestIP: dst.Unmap(),
			SourceResourceID: fields.GetSourceResourceId(), DestResourceID: fields.GetDestResourceId(),
			RxPackets: fields.GetRxPackets(), TxPackets: fields.GetTxPackets(), RxBytes: fields.GetRxBytes(), TxBytes: fields.GetTxBytes(),
			NumOfStarts: fields.GetNumOfStarts(), NumOfEnds: fields.GetNumOfEnds(), NumOfDrops: fields.GetNumOfDrops(),
		},
	}
	if ports := fields.GetPortInfo(); ports != nil {
		event.SourcePort, event.DestPort = uint16(ports.GetSourcePort()), uint16(ports.GetDestPort())
	}
	if icmp := fields.GetIcmpInfo(); icmp != nil {
		event.ICMPType, event.ICMPCode = uint8(icmp.GetIcmpType()), uint8(icmp.GetIcmpCode())
	}
	return event, nil
}

func timestamp(value time.Time) *timestamppb.Timestamp {
	return timestamppb.New(value)
}
