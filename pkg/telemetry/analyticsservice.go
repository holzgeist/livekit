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

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 . AnalyticsService
type AnalyticsService interface {
	SendStats(ctx context.Context, stats []*livekit.AnalyticsStat)
	SendEvent(ctx context.Context, events *livekit.AnalyticsEvent)
	SendNodeRoomStates(ctx context.Context, nodeRooms *livekit.AnalyticsNodeRooms)
}

type analyticsService struct {
	analyticsKey   string
	nodeID         string
	sequenceNumber atomic.Uint64

	events    rpc.AnalyticsRecorderService_IngestEventsClient
	stats     rpc.AnalyticsRecorderService_IngestStatsClient
	nodeRooms rpc.AnalyticsRecorderService_IngestNodeRoomStatesClient
}

func NewAnalyticsService(_ *config.Config, currentNode routing.LocalNode) AnalyticsService {
	service := &analyticsService{
		analyticsKey: "", // TODO: conf.AnalyticsKey
		nodeID:       currentNode.Id,
	}

	service.Connect()
	
	return service;
}

func (a *analyticsService) Connect() {
	// credentials, err := credentials.NewClientTLSFromFile("/etc/nginx/conf.d/instahelp.me.wildcard.crt", "tobias-dev.instahelp.me")
	// if err != nil {
	// 	logger.Errorw("credentials error", err);
	// 	return service;
	// }
	// conn, err := grpc.Dial("tobias-dev.instahelp.me:50052", grpc.WithTransportCredentials(credentials))
	conn, err := grpc.Dial("analytics-server:50052", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Errorw("couldn't connect to analytics server", err);
		return;
	}
	c := livekit.NewAnalyticsRecorderServiceClient(conn)
	// ctx := context.Background()
	stats, err := c.IngestStats(context.Background())
	if err != nil {
		logger.Errorw("failed to get stats client", err)
		a.stats = nil;
	} else {
		a.stats = stats;
	}

	events, err := c.IngestEvents(context.Background())
	if err != nil {
		logger.Errorw("failed to get events client", err)
		a.events = nil;
	} else {
		a.events = events;
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
	}
}

func (a *analyticsService) SendNodeRoomStates(_ context.Context, nodeRooms *livekit.AnalyticsNodeRooms) {
	if a.nodeRooms == nil {
		return
	}

	nodeRooms.NodeId = a.nodeID
	nodeRooms.SequenceNumber = a.sequenceNumber.Add(1)
	nodeRooms.Timestamp = timestamppb.Now()
	if err := a.nodeRooms.Send(nodeRooms); err != nil {
		logger.Errorw("failed to send node room states", err)
	}
}
