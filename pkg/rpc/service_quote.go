package rpc

import (
	"errors"
	"fmt"
	"math/big"
	"time"

	log "github.com/sirupsen/logrus"
	"gitlab.com/sdce/exlib/exutil"
	pb "gitlab.com/sdce/protogo"
	"gitlab.com/sdce/service/otc/pkg/api"
	"gitlab.com/sdce/service/otc/pkg/repository"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	apiCallLiveTime = time.Duration(1) * time.Second
)

func (o OtcServer) validateQuote(ctx context.Context, q *pb.Quote) (err error) {
	if q == nil {
		err = status.Errorf(codes.FailedPrecondition, "The quote is needed in the request")
	}
	coin := q.GetInstrument().GetQuote()
	if q.Side == pb.OrderSide_ASK {
		coin = q.GetInstrument().GetBase()
	}

	log.Infof("validating quote for coin %s", exutil.UUIDtoA(coin.GetId()))

	// if q.Side == pb.OrderSide_ASK && len(q.AcceptedPaymentMethods) == 0 {
	// 	err = errors.New("Accepted payment methods are needed when submitting sell quotes")
	// }

	if err != nil {
		log.Errorln("Precondition failed, quote cannot be created: " + err.Error())
		return
	}
	return
}

func (o OtcServer) DoCreateQuote(ctx context.Context, in *pb.CreateQuoteRequest) (*pb.CreateQuoteResponse, error) {
	// 1. rpc to member service to check member is valid and balance
	// 2. if buy side, check if relevant payment details is set up
	// 3. check transact password if used
	// 4. for sell side, rpc to member service to check balance is sufficient; for buy side, make sure it can meet the min volume
	// 5. call repository and write quote to db
	err := o.validateQuote(ctx, in.GetQuote())
	if err != nil {
		return nil, err
	}
	q := in.Quote
	//
	if q.Type == pb.Quote_WHOLESALE {
		q.MinValue = q.Value
		q.MaxValue = q.Value
		volf, ok := new(big.Float).SetString(q.Volume)
		if !ok {
			status.Errorf(codes.InvalidArgument, "The volume of the quote is not correct")
		}
		valf, ok := new(big.Float).SetString(q.Value)
		if !ok {
			status.Errorf(codes.InvalidArgument, "The value of the quote is not correct")
		}
		q.Price, _ = exutil.QuoFloat(valf, volf).Float64()

	} else {
		//calculate value
		pricef := new(big.Float).SetFloat64(q.Price)
		volf, ok := new(big.Float).SetString(q.Volume)
		if !ok {
			status.Errorf(codes.InvalidArgument, "The volume of the quote is not correct")
		}
		calcValf := exutil.MulFloat(pricef, volf)
		//round
		calcValf = exutil.AddFloat(calcValf, new(big.Float).SetFloat64(0.5))
		ival, _ := calcValf.Int(new(big.Int))
		q.Value = ival.String()
	}

	//status
	q.Status = pb.Quote_ON
	//fee
	rate, err := o.getOtcFeeRate(ctx, q.Owner)
	if err != nil {
		return nil, err
	}
	volFloat, err := exutil.Float(q.Volume)
	if err != nil {
		return nil, err
	}
	// lock neededVolume
	var neededVolume string
	if q.Side == pb.OrderSide_ASK {
		totalVolume := quo(volFloat, new(big.Float).Sub(big.NewFloat(1), rate))
		feeFloat := new(big.Float).Sub(totalVolume, volFloat)
		in.Quote.LockedFee = feeFloat.Text('f', 0)
		neededVolume = totalVolume.Text('f', 0)
	} else {
		neededVolume = q.Volume
		in.Quote.LockedFee = "0"
	}

	//MemberOtcDetail
	member, err := o.apis.FindMember(ctx, in.Quote.Owner)
	if err != nil {
		return nil, err
	}
	in.Quote.OwnerOtcDetail = member.OtcDetails
	//event
	q.Events = []*pb.OrderEvent{&pb.OrderEvent{
		Type:             pb.OrderEventType_CREATE_ORDER,
		Price:            q.Price,
		UpdateFromVolume: "0",
		UpdateFromValue:  "0",
		UpdateToVolume:   neededVolume,
		UpdateToValue:    q.Value,
		Time:             time.Now().UnixNano(),
	}}

	in.Quote.ProcessedVolume = "0"
	in.Quote.ProcessingVolume = "0"
	if in.Quote.Side == pb.OrderSide_ASK {
		in.Quote.VolumeToFill = in.GetQuote().GetVolume()
	} else {
		in.Quote.VolumeToFill = in.GetQuote().GetValue()
	}

	//Except buying coins with rmb, account balance needs to be locked
	if _, ok := externalCurrency[q.Instrument.Quote.Symbol]; !(ok && q.Side == pb.OrderSide_BID) {
		events := q.GetEvents()
		lr, err := api.MapQuoteToRequest(q, events[0])
		if err != nil {
			log.Errorf("failed to map quote to lock req: %v", err)
			return nil, err
		}
		err = o.apis.LockAccountBalance(ctx, lr)
		if err != nil {
			log.Errorf("Failed to lock account for quote: %s", exutil.UUIDtoA(q.GetId()))
			return nil, err
		}
	}

	//TODO: createquote and lockbalance should be put in a transaction
	qID, err := o.quotes.CreateQuote(ctx, in.Quote)
	if err != nil {
		log.Errorf("Failed to create quote: %v", err)
		return nil, err
	}

	res := &pb.CreateQuoteResponse{
		Id: qID,
	}
	return res, err
}

func (o OtcServer) DoListQuote(ctx context.Context, in *pb.ListQuoteRequest) (out *pb.ListQuoteResponse, err error) {
	filter := &repository.QuoteFilter{
		MemberId:      in.GetUserId(),
		Side:          in.GetSide(),
		Status:        in.GetStatus(),
		BaseCurrency:  in.GetBaseCurrency(),
		QuoteCurrency: in.GetQuoteCurrency(),
	}

	if in.GetPaging() != nil {
		filter.PageIdx = in.GetPaging().GetPageIndex()
		filter.PageSize = in.GetPaging().GetPageSize()
	}
	quotes, count, err := o.quotes.SearchQuotes(ctx, filter)
	if err != nil {
		log.Errorf("Search quotes: %v", err)
		return nil, status.Errorf(codes.NotFound, "Fail to search quotes")
	}
	out = &pb.ListQuoteResponse{
		Quotes:      quotes,
		ResultCount: count,
	}
	return
}

func (o OtcServer) DoGetQuoteDetails(ctx context.Context, in *pb.GetQuoteDetailsRequest) (out *pb.GetQuoteDetailsResponse, err error) {
	q, err := o.quotes.GetQuote(ctx, in.QuoteId)
	if err != nil {
		log.Errorf("Get Quote: %v", err)
		return nil, status.Errorf(codes.NotFound, "Fail to get quote")
	}
	out = &pb.GetQuoteDetailsResponse{
		Quote: q,
	}
	return
}

func (o OtcServer) DoUpdateQuote(ctx context.Context, in *pb.UpdateQuoteRequest) (out *pb.UpdateQuoteResponse, err error) {
	err = o.validateQuote(ctx, in.NewQuote)

	if in.NewQuote.Status != pb.Quote_ON {
		err = errors.New("quote is not on shelf which cannot be updated :" + in.NewQuote.Status.String())
	}

	q, err := o.quotes.GetQuote(ctx, in.NewQuote.Id)

	if err != nil || q == nil {
		log.Println("invalid quote id received, cannot update quote: " + in.NewQuote.Id.String())
		return
	}
	uobj, err := exutil.ApplyFieldMaskToBson(in.GetNewQuote(), in.GetUpdateMask())
	if err != nil {
		return
	}

	err = o.quotes.UpdateQuote(ctx, in.NewQuote.Id, uobj)
	if err != nil {
		return
	}
	out = &pb.UpdateQuoteResponse{
		Message: "Success",
	}
	return
}

func (o OtcServer) findOrderAccount(ctx context.Context, order *pb.OtcOrder, coinID *pb.UUID) (acc *pb.AccountDefined, err error) {
	accList, err := o.apis.FindMemberAccount(ctx, order.MemberId, coinID)

	if len(accList) == 0 || err != nil {
		err = fmt.Errorf("cannot find %s account for member: %s", coinID.String(), order.MemberId.String())
		return
	}
	acc = accList[0] // todo fixme
	return
}

func (o OtcServer) findQuoteAccount(ctx context.Context, q *pb.Quote, coinID *pb.UUID) (acc *pb.AccountDefined, err error) {
	if q == nil {
		err = fmt.Errorf("fatal: nil quote")
		return
	}

	accList, err := o.apis.FindMemberAccount(ctx, q.Owner, coinID)
	if err != nil {
		log.Error(err)
		return
	}
	if len(accList) == 0 {
		err = fmt.Errorf("cannot find %s account for member: %s", exutil.UUIDtoA(q.Instrument.Base.Id), exutil.UUIDtoA(q.Owner))
		log.Errorf("cannot find %s account for member: %s", exutil.UUIDtoA(q.Instrument.Base.Id), exutil.UUIDtoA(q.Owner))
		return
	}
	acc = accList[0] // todo fixme
	return
}

func (o OtcServer) DoDeleteQuote(ctx context.Context, in *pb.DeleteQuoteRequest) (out *pb.DeleteQuoteResponse, err error) {
	if in.Id == nil {
		err = fmt.Errorf("Invalid quote it received")
		return
	}
	q, err := o.quotes.GetQuote(ctx, in.Id)
	if err != nil {
		log.Error("Failed to find quote by id:" + in.Id.String())
		return
	}

	if q.Status == pb.Quote_CLOSED {
		log.Error("Quote has been closed already:" + exutil.UUIDtoA(in.Id))
		return nil, fmt.Errorf("Quote has been closed already:%s", exutil.UUIDtoA(in.Id))
	}
	if q.ProcessingVolume != "0" {
		log.Error("Can not cancel the quote with open otcorders")
		return nil, errors.New("Can not cancel the quote with open otcorders")
	}
	eventId := exutil.NewUUID()

	//TODO: should put release balance and deletequote in a transaction

	if q.Side == pb.OrderSide_ASK {
		acc, err := o.findQuoteAccount(ctx, q, q.Instrument.Base.Id)
		if err != nil {
			log.Errorf("Failed to find quote account for currency: %s", exutil.UUIDtoA(q.Instrument.Base.Id))
			return nil, err
		}
		//calculate fee+volume return
		qVolume, ok := new(big.Int).SetString(q.Volume, 10)
		if !ok {
			return nil, fmt.Errorf("can not transfer quoteVolume to int %s", q.Volume)
		}
		qLockedFee, ok := new(big.Int).SetString(q.LockedFee, 10)
		if !ok {
			return nil, fmt.Errorf("can not transfer quote LockedFee to int %s", q.LockedFee)
		}
		amount := new(big.Int).Add(qVolume, qLockedFee).String()

		if acc != nil {
			req := &pb.ReleaseLockedBalanceRequest{
				From:   acc.Id,
				To:     acc.Id,
				Amount: amount,
				Order: &pb.OrderRef{
					Id: in.Id,
				},
				Event: &pb.OrderEvent{
					Id: eventId,
				},
			}
			err = o.apis.ReleaselockedBalance(ctx, req)

			if err != nil {
				log.Error("failed to release locked balance: from " + exutil.UUIDtoA(acc.Id) + " to " + exutil.UUIDtoA(acc.Id))
				return nil, err
			}
		}
	}

	if _, ok := externalCurrency[q.Instrument.Quote.Symbol]; !ok && q.Side == pb.OrderSide_BID {
		acc, err := o.findQuoteAccount(ctx, q, q.Instrument.Quote.Id)
		if err != nil {
			log.Errorf("Failed to find quote account for currency: %s", exutil.UUIDtoA(q.Instrument.Quote.Id))
			return nil, err
		}
		if acc != nil {
			req := &pb.ReleaseLockedBalanceRequest{
				From:   acc.Id,
				To:     acc.Id,
				Amount: q.Value,
				Order: &pb.OrderRef{
					Id: q.Id,
				},
				Event: &pb.OrderEvent{
					Id: eventId,
				},
			}
			err = o.apis.ReleaselockedBalance(ctx, req)

			if err != nil {
				log.Error("failed to release locked balance: from " + exutil.UUIDtoA(acc.Id) + " to " + exutil.UUIDtoA(acc.Id))
				return nil, err
			}
		}
	}

	err = o.quotes.DeleteQuote(ctx, in.Id, eventId)
	if err == nil {
		out = &pb.DeleteQuoteResponse{
			Message: "Success",
		}
	}
	return
}

/*
 put on/off quote onto shelf
*/

func (o OtcServer) DoCreateSDCEQuote(ctx context.Context, in *pb.SDCEQuoteRequest) (*pb.SDCEQuoteResponse, error) {
	quote := in.GetQuote()

	err := o.quotes.CreateSDCEQuote(ctx, quote.GetTicker(), quote.GetBuyUnitPrice(), quote.GetSellUnitPrice())
	if err != nil {
		log.Errorf("DoCreateSDCEQuote error : %v", err)
		return nil, err
	}

	return &pb.SDCEQuoteResponse{}, nil
}

func (o OtcServer) DoGetSDCEQuote(ctx context.Context, in *pb.GetSDCEQuoteRequest) (out *pb.GetSDCEQuoteResponse, err error) {
	resp, err := o.quotes.SearchSDCEQuote(ctx, in.GetTicker())
	if err != nil {
		log.Errorf("DoGetSDCEQuote error : %v", err)
		return
	}
	out = &pb.GetSDCEQuoteResponse{
		Quotes: resp,
	}
	log.Infof("get sdce quote,response : %v", resp)
	return
}
