package otcapi

import (
	"context"
	"fmt"
	"time"

	"gitlab.com/sdce/exlib/service"
	pb "gitlab.com/sdce/protogo"
	"google.golang.org/grpc"
)

const (
	apiCallLiveTime = 5 * time.Second
)

type OTCApi interface {
	UpdateOrder(ctx context.Context, in *pb.UpdateOtcOrderStatusRequest) (err error)
}

type Server struct {
	OTC pb.OtcTradingClient
}

func newOTCClient(otcURL string) (pb.OtcTradingClient, error) {
	// Set up a connection to the server.
	fmt.Println("OTC grpc host:" + otcURL)
	conn, err := grpc.Dial(otcURL, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	return pb.NewOtcTradingClient(conn), nil
}

//New api server
func New(config *service.Config) (OTCApi, error) {
	otc, err := newOTCClient(config.Otc)
	if err != nil {
		return nil, err
	}
	return &Server{
		OTC: otc,
	}, nil
}

func (o Server) UpdateOrder(ctx context.Context, in *pb.UpdateOtcOrderStatusRequest) (err error) {
	apiCtx, cancel := context.WithTimeout(ctx, apiCallLiveTime)
	defer cancel()
	_, err = o.OTC.DoUpdateOrder(apiCtx, in)
	if err != nil {
		return err
	}
	return nil
}
