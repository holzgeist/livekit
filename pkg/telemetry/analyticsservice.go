// Copyright 2023 LiveKit, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package telemetry

import (
	"context"
	"time"

	"go.uber.org/atomic"
	"google.golang.org/protobuf/types/known/timestamppb"

	"google.golang.org/grpc"
	// "google.golang.org/grpc/credentials/local"
	"google.golang.org/grpc/credentials/insecure"
	// "google.golang.org/grpc/credentials"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/protocol/rpc"
	"github.com/livekit/protocol/utils/guid"

	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/routing"
)

//counterfeiter:generate . AnalyticsService
type AnalyticsService interface {
	SendStats(ctx context.Context, stats []*livekit.AnalyticsStat)
	SendEvent(ctx context.Context, events *livekit.AnalyticsEvent)
	SendNodeRoomStates(ctx context.Context, nodeRooms *livekit.AnalyticsNodeRooms)
}

type analyticsService struct {
	analyticsKey   string
	nodeID         string
	sequenceNumber atomic.Uint64
	analyticsHost  string

	lastConnectAttempt *time.Time

	events    rpc.AnalyticsRecorderService_IngestEventsClient
	stats     rpc.AnalyticsRecorderService_IngestStatsClient
	nodeRooms rpc.AnalyticsRecorderService_IngestNodeRoomStatesClient
}

func NewAnalyticsService(conf *config.Config, currentNode routing.LocalNode) AnalyticsService {
	service := &analyticsService{
		analyticsKey:       "", // TODO: conf.AnalyticsKey
		nodeID:             string(currentNode.NodeID()),
		analyticsHost:      conf.AnalyticsHost,
		lastConnectAttempt: nil,
	}

	service.Connect()

	return service
}

func (a *analyticsService) Connect() {
	if a.analyticsHost == "" {
		return
	}

	if a.lastConnectAttempt != nil {
		// only try to connect once a minute
		if a.lastConnectAttempt.Add(60*time.Second).Compare(time.Now().UTC()) > 0 {
			logger.Debugw("not connecting to analytics server again", "last attempt", a.lastConnectAttempt)
			return
		}
	}

	now := time.Now().UTC()
	a.lastConnectAttempt = &now

	logger.Infow("Connecting to analytics server")
	conn, err := grpc.NewClient(
		a.analyticsHost,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{
			MinConnectTimeout: time.Second * 2,
		}),
	)
	if err != nil {
		logger.Errorw("couldn't connect to analytics server", err)
		return
	}
	c := rpc.NewAnalyticsRecorderServiceClient(conn)
	stats, err := c.IngestStats(context.Background())
	if err != nil {
		logger.Warnw("failed to get stats client", err)
		a.stats = nil
	} else {
		a.stats = stats
	}

	events, err := c.IngestEvents(context.Background())
	if err != nil {
		logger.Warnw("failed to get events client", err)
		a.events = nil
	} else {
		a.events = events
	}

	nodeRooms, err := c.IngestNodeRoomStates(context.Background())
	if err != nil {
		logger.Warnw("failed to get nodeRooms client", err)
		a.nodeRooms = nil
	} else {
		a.nodeRooms = nodeRooms
	}

	if a.stats != nil && a.events != nil && a.nodeRooms != nil {
		logger.Infow("Connected to analytics server")
	} else {
		logger.Warnw("Could not connect to the analytics server, trying again in 60s", nil)
		conn.Close()
		a.stats = nil
		a.events = nil
		a.nodeRooms = nil
	}
}

func (a *analyticsService) SendStats(_ context.Context, stats []*livekit.AnalyticsStat) {
	if a.stats == nil {
		a.Connect()
		if a.stats == nil {
			return
		}
	}

	for _, stat := range stats {
		stat.Id = guid.New("AS_")
		stat.AnalyticsKey = a.analyticsKey
		stat.Node = a.nodeID
	}
	if err := a.stats.Send(&livekit.AnalyticsStats{Stats: stats}); err != nil {
		logger.Errorw("failed to send stats", err)
		a.stats = nil
		a.Connect()
		if a.stats == nil {
			return
		}
		a.stats.Send(&livekit.AnalyticsStats{Stats: stats})
	}
}

func (a *analyticsService) SendEvent(_ context.Context, event *livekit.AnalyticsEvent) {
	if a.events == nil {
		a.Connect()
		if a.events == nil {
			return
		}
	}

	event.Id = guid.New("AE_")
	event.NodeId = a.nodeID
	event.AnalyticsKey = a.analyticsKey
	if err := a.events.Send(&livekit.AnalyticsEvents{
		Events: []*livekit.AnalyticsEvent{event},
	}); err != nil {
		logger.Errorw("failed to send event", err, "eventType", event.Type.String())
		a.events = nil
		a.Connect()
		if a.events == nil {
			return
		}
		a.events.Send(&livekit.AnalyticsEvents{
			Events: []*livekit.AnalyticsEvent{event},
		})
	}
}

func (a *analyticsService) SendNodeRoomStates(_ context.Context, nodeRooms *livekit.AnalyticsNodeRooms) {
	if a.nodeRooms == nil {
		a.Connect()
		if a.nodeRooms == nil {
			return
		}
	}

	nodeRooms.NodeId = a.nodeID
	nodeRooms.SequenceNumber = a.sequenceNumber.Add(1)
	nodeRooms.Timestamp = timestamppb.Now()
	if err := a.nodeRooms.Send(nodeRooms); err != nil {
		logger.Errorw("failed to send nodeRooms", err)
		a.nodeRooms = nil
		a.Connect()
		if a.nodeRooms == nil {
			return
		}
		a.nodeRooms.Send(nodeRooms)
	}
}
