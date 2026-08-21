package server

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	gstatus "google.golang.org/grpc/status"

	"github.com/netbirdio/netbird/client/proto"
)

// ListRelays returns the relays received by the running client with local probe results.
func (s *Server) ListRelays(ctx context.Context, _ *proto.EmptyRequest) (*proto.ListRelaysResponse, error) {
	s.mutex.Lock()
	connectClient := s.connectClient
	s.mutex.Unlock()

	if connectClient == nil || connectClient.Engine() == nil {
		return nil, fmt.Errorf("not connected")
	}

	relays := connectClient.Engine().ProbeRelayServers(ctx)
	response := &proto.ListRelaysResponse{
		Relays: make([]*proto.RelayServer, 0, len(relays)),
	}
	for _, relay := range relays {
		response.Relays = append(response.Relays, &proto.RelayServer{
			Uri:       relay.URL,
			Weight:    int32(relay.Weight),
			Preferred: relay.Preferred,
			Forced:    relay.Forced,
			Current:   relay.Current,
			Available: relay.Available,
			Error:     relay.Error,
		})
	}

	return response, nil
}

// SetRelay forces the running client to use a specific received relay.
func (s *Server) SetRelay(_ context.Context, req *proto.SetRelayRequest) (*proto.SetRelayResponse, error) {
	s.mutex.Lock()
	connectClient := s.connectClient
	s.mutex.Unlock()

	if connectClient == nil || connectClient.Engine() == nil {
		return nil, fmt.Errorf("not connected")
	}

	selected, err := connectClient.Engine().SetForcedRelay(req.GetRelay())
	if err != nil {
		return nil, gstatus.Errorf(codes.InvalidArgument, "%s", err)
	}

	return &proto.SetRelayResponse{Selected: selected}, nil
}
