package api

import (
	"context"
	"log"
	"math/big"
	"time"

	"gitlab.com/sdce/exlib/exutil"
	pb "gitlab.com/sdce/protogo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type LockBalance struct {
	FromAmount *big.Int
	ToAmount   *big.Int
	CoinId     *pb.UUID
	MemberId   *pb.UUID
	ActivityId *pb.UUID
	Source     pb.ActivitySource
}

func MapQuoteToRequest(q *pb.Quote, event *pb.OrderEvent) (req *LockBalance, err error) {
	var (
		coinID               *pb.UUID
		fromAmount, toAmount *big.Int
	)

	req = &LockBalance{}
	side := q.GetSide()
	instrument := q.GetInstrument()

	switch side {
	case pb.OrderSide_ASK:
		coinID = instrument.GetBase().GetId()
		fromAmount, toAmount, err = exutil.DecodeTwoBigInts(event.GetUpdateFromVolume(), event.GetUpdateToVolume())
		if err != nil {
			err = status.Errorf(codes.InvalidArgument,
				"newLockBalanceReq: invalid event volume format (%v, %v)",
				event.GetUpdateToVolume(), event.GetUpdateToVolume())
			return
		}
	case pb.OrderSide_BID:
		coinID = instrument.GetQuote().GetId()
		fromAmount, toAmount, err = exutil.DecodeTwoBigInts(event.GetUpdateFromValue(), event.GetUpdateToValue())
		if err != nil {
			err = status.Errorf(codes.InvalidArgument,
				"newLockBalanceReq: invalid event value format (%v, %v)",
				event.GetUpdateFromValue(), event.GetUpdateToValue())
			return
		}
	default:
		err = grpc.Errorf(codes.InvalidArgument, "newLockBalanceReq: invalid side")
		return
	}
	req.CoinId = coinID
	req.FromAmount = fromAmount
	req.ToAmount = toAmount
	req.MemberId = q.GetOwner()
	req.ActivityId = q.GetId()
	req.Source = pb.ActivitySource_ORDER
	return
}

func (s *Server) newLockBalanceRequest(ctx context.Context, lr *LockBalance) (req *pb.LockBalanceRequest, err error) {

	var account *pb.AccountDefined
	{
		var ctx, cancel = context.WithTimeout(ctx, time.Minute)
		defer cancel()
		var resp *pb.SearchAccountsResponse
		if s.Member == nil {
			log.Fatal("nil member client configured in otc server")
		}
		resp, err = s.Member.DoSearchAccounts(ctx, &pb.SearchAccountsRequest{
			MemberId:   lr.MemberId,
			Currencies: []*pb.UUID{lr.CoinId},
			Paging:     &pb.PaginationRequest{PageIndex: 0, PageSize: 1},
		})
		if err != nil {
			err = status.Errorf(codes.Unavailable, "newLockBalanceReq: failed to find account for currency %v", lr.CoinId)
			return
		} else if len(resp.Account) == 0 {
			err = status.Errorf(codes.NotFound, "newLockBalanceReq: no account found for currency %v", lr.CoinId)
			return
		}
		account = resp.Account[0]
	}

	amount := new(big.Int).Sub(lr.ToAmount, lr.FromAmount)

	// cannot change as more balance has been locked
	if amount.Sign() <= 0 {
		// TODO should not throw error when updating to less amount
		return nil, grpc.Errorf(codes.PermissionDenied, "balance has been locked!")
	}

	// build lock req
	req = &pb.LockBalanceRequest{
		CurrencyId: lr.CoinId,
		Amount:     amount.Text(10),
		MemberId:   lr.MemberId,
		AccountId:  account.GetId(),
		ActivityId: lr.ActivityId,
		Source:     pb.ActivitySource_ORDER,
	}
	return
}
