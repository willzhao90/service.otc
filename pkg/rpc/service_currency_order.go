package rpc

import (
	"fmt"
	"math/big"
	"time"

	"gitlab.com/sdce/exlib/exutil"
	"gitlab.com/sdce/service/otc/pkg/api"
	"gitlab.com/sdce/service/otc/pkg/repository"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	log "github.com/sirupsen/logrus"
	exmongo "gitlab.com/sdce/exlib/mongo"
	pb "gitlab.com/sdce/protogo"
	"golang.org/x/net/context"
)

func (o OtcServer) DoCreateCurrencyOrder(ctx context.Context, in *pb.CreateCurrencyOrderRequest) (*pb.CreateCurrencyOrderResponse, error) {
	//Calculate expired time for order
	if in.CurrencyOrder.GetClientId() == "" {
		err := status.Errorf(codes.InvalidArgument, "There should be a client id to create currency order.")
		log.Error(err)
		return nil, err
	}
	liveTime, err := o.getOrderLiveTime(ctx, in.CurrencyOrder.GetClientId())
	if err != nil {
		return nil, err
	}
	if in.CurrencyOrder.GetCreatedAt() == 0 {
		err := status.Errorf(codes.InvalidArgument, "Created time is needed to create currency order.")
		log.Error(err)
		return nil, err
	}
	createdTime := time.Unix(0, in.CurrencyOrder.GetCreatedAt())
	var expiredTime int64
	if liveTime == -1 {
		expiredTime = time.Date(2199, time.April, 1, 1, 1, 1, 1, time.UTC).UnixNano()
	} else {
		expiredTime = createdTime.Add(time.Duration(liveTime) * time.Second).UnixNano()
	}

	in.CurrencyOrder.ExpiredTime = expiredTime

	cID, err := o.currencyorders.CreateCurrencyOrder(ctx, in.CurrencyOrder)
	if err != nil {
		log.Errorf("Failed to create currency order: %v", err)
		err = exmongo.ErrorToRpcError(err)
		return nil, exmongo.ErrorToRpcError(err)
	}

	res := &pb.CreateCurrencyOrderResponse{
		Id: cID,
	}
	return res, err
}

func (o OtcServer) DoGetCurrencyOrder(ctx context.Context, in *pb.GetCurrencyOrderRequest) (*pb.GetCurrencyOrderResponse, error) {
	currencyOrder, err := o.currencyorders.GetCurrencyOrder(ctx, in.CurrencyOrderId)
	if err != nil {
		log.Errorf("Failed to get currency order: &v", err)
		err = exmongo.ErrorToRpcError(err)
		return nil, err
	}
	res := &pb.GetCurrencyOrderResponse{
		CurrencyOrder: currencyOrder,
	}
	return res, err
}

func (o OtcServer) DoUpdateCurrencyOrder(ctx context.Context, in *pb.UpdateCurrencyOrderRequest) (*pb.UpdateCurrencyOrderResponse, error) {
	inOrder := in.Currencyorder
	currencyOrder, err := o.currencyorders.GetCurrencyOrder(ctx, inOrder.Id)
	if err != nil {
		log.Errorf("Failed to get currency order: &v", err)
		err = exmongo.ErrorToRpcError(err)
		return nil, err
	}
	out := &pb.UpdateCurrencyOrderResponse{}
	//status validation
	if currencyOrder.Status == pb.CurrencyOrder_EXPIRED || currencyOrder.Status == pb.CurrencyOrder_REJECTED || currencyOrder.Status == pb.CurrencyOrder_SETTLED {
		err = status.Errorf(codes.PermissionDenied, "currency order in status : %v can not be changed", currencyOrder.Status)
	}

	switch inOrder.Status {
	case pb.CurrencyOrder_OPEN:
		if currencyOrder.Status != pb.CurrencyOrder_INITIATED {
			err = status.Errorf(codes.PermissionDenied, "currency order in status : %v can not be changed to open", currencyOrder.Status)
			return nil, err
		}
		//lock balance if in aud/usd
		// if strings.Contains(currencyOrder.GetTicker(), "cny") && currencyOrder.GetSide() == pb.CurrencyOrder_BUY {
		// 	err = o.currencyOrderLockBalance(ctx, currencyOrder)
		// 	if err != nil {
		// 		err = status.Errorf(codes.FailedPrecondition, "Fail to lock balance: %v", err)
		// 		return nil, err
		// 	}
		// }

		timeMemo := exutil.GetTimeMemo()
		inOrder.Memo = timeMemo
		out.Memo = timeMemo
	case pb.CurrencyOrder_REVIEW:
		if currencyOrder.Status != pb.CurrencyOrder_OPEN {
			err = status.Errorf(codes.PermissionDenied, "currency order in status : %v can not be changed to paid", currencyOrder.Status)
			return nil, err
		}
	case pb.CurrencyOrder_REVIEWED:
		if currencyOrder.Status != pb.CurrencyOrder_REVIEW {
			err = status.Errorf(codes.PermissionDenied, "currency order in status : %v can not be changed to paid", currencyOrder.Status)
			return nil, err
		}
	case pb.CurrencyOrder_PAID:
		if currencyOrder.Status != pb.CurrencyOrder_OPEN && currencyOrder.Status != pb.CurrencyOrder_REVIEW && currencyOrder.Status != pb.CurrencyOrder_INITIATED && currencyOrder.Status != pb.CurrencyOrder_REVIEWED {
			err = status.Errorf(codes.PermissionDenied, "currency order in status : %v can not be changed to paid", currencyOrder.Status)
			return nil, err
		}
	case pb.CurrencyOrder_COMPLETED:
		if currencyOrder.Status != pb.CurrencyOrder_PAID {
			err = status.Errorf(codes.PermissionDenied, "currency order in status : %v can not be changed to completed", currencyOrder.Status)
			return nil, err
		}
		//release balance
		// if strings.Contains(currencyOrder.GetTicker(), "cny") && currencyOrder.GetSide() == pb.CurrencyOrder_BUY {
		// 	err = o.currencyOrderReleaseBalance(ctx, currencyOrder, "COMPLETED")
		// 	if err != nil {
		// 		err = status.Errorf(codes.FailedPrecondition, "Fail to release balance: %v", err)
		// 		return nil, err
		// 	}
		// }

	case pb.CurrencyOrder_SETTLED:
		if currencyOrder.Status != pb.CurrencyOrder_COMPLETED {
			err = status.Errorf(codes.PermissionDenied, "currency order in status : %v can not be changed to settled", currencyOrder.Status)
			return nil, err
		}
	case pb.CurrencyOrder_REJECTED:
		//TODO:

	default:
		err = status.Errorf(codes.InvalidArgument, "Invalid status received")
		return nil, err
	}

	err = o.currencyorders.UpdateCurrencyOrder(ctx, inOrder)
	if err != nil {
		log.Errorf("Failed to update currency order: %v", err)
		err = exmongo.ErrorToRpcError(err)
		return nil, err
	}
	return out, err
}

func (o OtcServer) DoSearchCurrencyOrders(ctx context.Context, in *pb.SearchCurrencyOrdersRequest) (out *pb.SearchCurrencyOrdersResponse, err error) {
	filter := &repository.CurrencyOrderFilter{
		OwnerName:      in.GetOwnerName(),
		Ticker:         in.GetTicker(),
		Status:         in.GetStatus(),
		Side:           in.GetSide(),
		OwnerId:        in.GetOwnerId(),
		FromTime:       in.FromTime,
		ToTime:         in.ToTime,
		OwnerWalletUID: in.OwnerWalletUID,
	}
	if in.GetPaging() != nil {
		filter.PageIdx = in.GetPaging().GetPageIndex()
		filter.PageSize = in.GetPaging().GetPageSize()
	}

	currencyOrders, count, err := o.currencyorders.SearchCurrencyOrders(ctx, filter)
	if err != nil {
		log.Errorf("search currency order err: %v", err)
		return nil, exmongo.ErrorToRpcError(err)
	}

	out = &pb.SearchCurrencyOrdersResponse{
		Orders:      currencyOrders,
		ResultCount: count,
	}
	return
}

func (o OtcServer) getOrderLiveTime(ctx context.Context, clientId string) (int32, error) {
	var liveTime int32
	merchant, count, err := o.merchants.SearchMerchant(ctx, &repository.MerchantFilter{ClientId: clientId})
	if err != nil {
		log.Errorf("Fail to search related merchant by client id : %s err: %v", clientId, err)
		err = exmongo.ErrorToRpcError(err)
		return 0, err
	}
	if count != 1 {
		liveTime = 30 * 60
	} else {
		liveTime = merchant[0].GetOrderLiveTime()
	}

	if liveTime == 0 {
		err = status.Errorf(codes.Internal, "Merchant's order live time should not be 0, merchant client id: %s", clientId)
		log.Error(err)
		return 0, err
	}
	return liveTime, nil
}

func (o OtcServer) currencyOrderLockBalance(ctx context.Context, currencyOrder *pb.CurrencyOrder) (err error) {
	account, err := o.apis.FindMemberAccount(ctx, currencyOrder.GetOwner().GetId(), currencyOrder.GetCurrencyQuote().GetQuantity().GetCurrency().GetId())
	if err != nil {
		log.Errorf("Fail to find account:%v", err)
		return
	}
	if len(account) != 1 {
		err = fmt.Errorf("Can not find only one account: %d", len(account))
		return err
	}

	eventId := exutil.NewUUID()
	toAmount, err := exutil.DecodeBigInt(currencyOrder.GetCurrencyQuote().GetQuantity().GetQuantity())
	if err != nil {
		err = fmt.Errorf("Can not transfer currency order amount:%v", err)
		return err
	}
	lr := &api.LockBalance{
		FromAmount: big.NewInt(0),
		ToAmount:   toAmount,
		MemberId:   currencyOrder.GetOwner().GetId(),
		CoinId:     currencyOrder.GetCurrencyQuote().GetQuantity().GetCurrency().GetId(),
		ActivityId: eventId,
		Source:     pb.ActivitySource_ORDER,
	}

	err = o.apis.LockAccountBalance(ctx, lr)
	return
}

//toStatus COMPLETE/REJECTED
func (o OtcServer) currencyOrderReleaseBalance(ctx context.Context, currencyOrder *pb.CurrencyOrder, toStatus string) (err error) {
	account, err := o.apis.FindMemberAccount(ctx, currencyOrder.GetOwner().GetId(), currencyOrder.GetCurrencyQuote().GetQuantity().GetCurrency().GetId())
	if err != nil {
		log.Errorf("Fail to find account:%v", err)
		return
	}
	if len(account) != 1 {
		err = fmt.Errorf("Can not find only one account: %d", len(account))
		return err
	}

	eventId := exutil.NewUUID()
	req := &pb.ReleaseLockedBalanceRequest{
		From:   account[0].Id,
		To:     nil,
		Amount: currencyOrder.GetCurrencyQuote().GetQuantity().GetQuantity(),
		Order: &pb.OrderRef{
			Id: currencyOrder.Id,
		},
		Event: &pb.OrderEvent{
			Id: eventId,
		},
	}
	if toStatus == "REJECTED" {
		req.To = account[0].Id
	}
	o.apis.ReleaselockedBalance(ctx, req)
	return
}
