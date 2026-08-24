package grpc

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	"github.com/netbirdio/netbird/management/server/account"
	"github.com/netbirdio/netbird/management/server/store"
)

func TestFlowServerCleanupFailureIsRecoverable(t *testing.T) {
	ctrl := gomock.NewController(t)
	manager := account.NewMockManager(ctrl)
	dbStore := store.NewMockStore(ctrl)
	manager.EXPECT().GetStore().Return(dbStore).AnyTimes()
	dbStore.EXPECT().CleanupNetworkTrafficEvents(gomock.Any(), gomock.Any(), 10).
		Return(int64(0), errors.New("database unavailable")).Times(1)
	dbStore.EXPECT().CleanupNetworkTrafficEvents(gomock.Any(), gomock.Any(), 10).
		Return(int64(3), nil).Times(1)

	server := NewFlowServer(manager)
	server.cleanup(context.Background(), time.Hour, 10)
	server.cleanup(context.Background(), time.Hour, 10)
}
