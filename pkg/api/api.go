package api

import (
	"context"
	"fmt"

	"gitlab.com/sdce/exlib/service"
	pb "gitlab.com/sdce/protogo"
	"google.golang.org/grpc"
)

type Api interface {
	LockAccountBalance(ctx context.Context, lr *LockBalance) (err error)
	ReleaselockedBalance(ctx context.Context, req *pb.ReleaseLockedBalanceRequest) (err error)
	FindMemberAccount(ctx context.Context, member *pb.UUID, coin *pb.UUID) ([]*pb.AccountDefined, error)
	FindMember(ctx context.Context, memberId *pb.UUID) (*pb.MemberDefined, error)
	FindInstrument(ctx context.Context, code string) (*pb.Instrument, error)
	AddPending(ctx context.Context, in *pb.AddPendingRequest) (*pb.AddPendingResponse, error)
	ReleasePending(ctx context.Context, in *pb.ReleasePendingRequest) (*pb.ReleasePendingResponse, error)
}

//Server serving for rpc api
type Server struct {
	Member  pb.MemberClient
	Trading pb.TradingClient
}

func newMemberClient(memberURL string) (pb.MemberClient, error) {
	// Set up a connection to the server.
	fmt.Println("Member grpc host:" + memberURL)
	conn, err := grpc.Dial(memberURL, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	return pb.NewMemberClient(conn), nil
}
func newTradingClient(tradingURL string) (pb.TradingClient, error) {
	// Set up a connection to the server.
	fmt.Println("Member grpc host:" + tradingURL)
	conn, err := grpc.Dial(tradingURL, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	return pb.NewTradingClient(conn), nil
}

//New api server
func New(config *service.Config) (Api, error) {
	member, err := newMemberClient(config.Member)
	if err != nil {
		return nil, err
	}
	trading, err := newTradingClient((config.Trading))
	return &Server{
		Member:  member,
		Trading: trading,
	}, nil
}
