package rpc

import (
	"fmt"
	"math/big"
	"strconv"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"gitlab.com/sdce/service/otc/pkg/repository"

	"gitlab.com/sdce/service/otc/pkg/api"

	log "github.com/sirupsen/logrus"
	"gitlab.com/sdce/exlib/exutil"
	pb "gitlab.com/sdce/protogo"
	"go.mongodb.org/mongo-driver/bson"
	"golang.org/x/net/context"
)

const feeRate = 0.002

func (o OtcServer) DoGetOtcOrder(ctx context.Context, in *pb.GetOtcOrderRequest) (out *pb.GetOtcOrderResponse, err error) {
	order, err := o.trades.GetOtcOrder(ctx, in.OrderId)
	if err != nil {
		log.Errorf("Get otc order err: %v", err)
		return nil, status.Errorf(codes.NotFound, "Fail to get otc order")
	}
	out = &pb.GetOtcOrderResponse{
		Order: order,
	}
	return
}

func (o OtcServer) DoListOrder(ctx context.Context, in *pb.ListOtcOrderRequest) (out *pb.ListOtcOrderResponse, err error) {

	filter := &repository.OrderFilter{
		MemberId:      in.GetUserId(),
		Status:        in.GetStatus(),
		Side:          in.GetSide(),
		BaseCurrency:  in.GetBaseCurrency(),
		QuoteCurrency: in.GetQuoteCurrency(),
	}
	if in.GetPaging() != nil {
		filter.PageIdx = in.GetPaging().GetPageIndex()
		filter.PageSize = in.GetPaging().GetPageSize()
	}

	orders, count, err := o.trades.SearchOtcOrders(ctx, filter)
	if err != nil {
		log.Errorf("search otc order err: &v", err)
		return nil, status.Errorf(codes.NotFound, "Fail to search otc orders")
	}

	out = &pb.ListOtcOrderResponse{
		Orders:      orders,
		ResultCount: count,
	}
	return
}

func (o OtcServer) DoCancelOrder(ctx context.Context, in *pb.CancelOtcOrderRequest) (out *pb.CancelOtcOrderResponse, err error) {
	//Check unpaid status
	order, err := o.trades.GetOtcOrder(ctx, in.OrderId)
	if err != nil {
		log.Errorf("Invalid order ID")
		return
	}
	if order.Status != pb.OtcOrder_UNPAID {
		err := fmt.Errorf("The order can only be canceled when the order is in unpaid status.")
		log.Errorf(err.Error())
		return nil, err
	}
	eventId := exutil.NewUUID()
	//Release locked account of the order
	if _, ok := externalCurrency[order.Instrument.Quote.Symbol]; !(ok && order.Side == pb.OrderSide_BID) {
		var coinId *pb.UUID
		var amount string
		if order.Side == pb.OrderSide_ASK {
			coinId = order.GetInstrument().GetBase().Id
			amount = order.Volume
		} else {
			coinId = order.GetInstrument().GetQuote().Id
			amount = order.Value
		}

		orderAccount, err := o.findOrderAccount(ctx, order, coinId)
		if err != nil {
			log.Errorf(err.Error())
			return nil, err
		}
		req := &pb.ReleaseLockedBalanceRequest{
			From:   orderAccount.Id,
			To:     orderAccount.Id,
			Amount: amount,
			Order: &pb.OrderRef{
				Id: order.Id,
			},
			Event: &pb.OrderEvent{
				Id: eventId,
			},
		}
		err = o.apis.ReleaselockedBalance(ctx, req)
		if err != nil {
			log.Error("fail to release locked balance")
			return nil, err
		}
	}
	//release pending
	err = o.releasePendingPro(ctx, order)
	if err != nil {
		log.Errorf("release pending error: %v", err)
		return nil, err
	}

	//update quote volume and value
	q, err := o.quotes.GetQuote(ctx, order.QuoteId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "failed to find requested quote id: %v", exutil.UUIDtoA(order.QuoteId))
	}
	err = o.updateQuoteVolumeValueandFee(ctx, order.Volume, order.Value, order.Fee, q, "CANCEL")
	if err != nil {
		log.Errorf("Fail to update quote volume and value when cancel order")
		return nil, err
	}

	err = o.trades.DeleteOtcOrder(ctx, in.OrderId)
	if err != nil {
		log.Errorln("Failed to update order:" + in.OrderId.String())
		return
	}
	out = &pb.CancelOtcOrderResponse{
		Message: "Success",
	}
	return
}

func (o OtcServer) DoUpdateOrder(ctx context.Context, in *pb.UpdateOtcOrderStatusRequest) (out *pb.UpdateOtcOrderStatusResponse, err error) {
	order, err := o.trades.GetOtcOrder(ctx, in.OrderId)
	if err != nil {
		log.Errorf("cannot find order: %s", exutil.UUIDtoA(in.OrderId))
		return nil, err
	}
	if order.Status == in.Status {
		log.Info("The Status does not need to be changed")
		out = &pb.UpdateOtcOrderStatusResponse{
			Message: "The Status does not need to be changed",
		}
		return out, nil
	}
	switch order.Status {
	case pb.OtcOrder_CANCELLED:
		err = fmt.Errorf("status of Cancelled order cannot be changed ")
		return nil, err
	case pb.OtcOrder_RESOLVED:
		err = fmt.Errorf("status of Resolved order cannot be changed ")
		return nil, err
	case pb.OtcOrder_APPEAL:
		if in.Status != pb.OtcOrder_RESOLVED && in.Status != pb.OtcOrder_CANCELLED {
			err = fmt.Errorf("appealed order can only be resolved or cancelled")
			return nil, err
		}
	case pb.OtcOrder_UNPAID:
		if in.Status != pb.OtcOrder_PAID && in.Status != pb.OtcOrder_APPEAL && in.Status != pb.OtcOrder_CANCELLED && in.Status != pb.OtcOrder_EXPIRED {
			err = fmt.Errorf("appealed order can only be resolved or cancelled")
			return nil, err
		}
	case pb.OtcOrder_PAID:
		if in.Status != pb.OtcOrder_COMPLETED && in.Status != pb.OtcOrder_APPEAL && in.Status != pb.OtcOrder_CANCELLED && in.Status != pb.OtcOrder_UNPAID {
			err = fmt.Errorf("appealed order can only be resolved or cancelled")
			return nil, err
		}

	case pb.OtcOrder_COMPLETED:
		if in.Status != pb.OtcOrder_COMPLETED && in.Status != pb.OtcOrder_APPEAL && in.Status != pb.OtcOrder_CANCELLED {
			err = fmt.Errorf("appealed order can only be resolved or cancelled")
			return nil, err
		}
	}
	eventId := exutil.NewUUID()

	//Pay and Release
	if order.Status == pb.OtcOrder_UNPAID && in.Status == pb.OtcOrder_PAID {
		err := o.payOrder(ctx, order, eventId)
		if err != nil {
			log.Errorf("Fail to pay Order %s", err.Error())
			return nil, err
		}

	}

	if order.Status == pb.OtcOrder_PAID && in.Status == pb.OtcOrder_UNPAID {
		err := o.refundOrder(ctx, order, eventId)
		if err != nil {
			log.Errorf("Fail to refund Order %s", err.Error())
			return nil, err
		}
	}

	if order.Status == pb.OtcOrder_PAID && in.Status == pb.OtcOrder_COMPLETED {
		err := o.releaseCoin(ctx, order, eventId)
		if err != nil {
			log.Errorf("Fail to release coin %s", err.Error())
			return nil, err
		}
		//release pending
		err = o.releasePendingPro(ctx, order)
		if err != nil {
			log.Errorf("release pending error: %v", err)
			return nil, err
		}
		//update quote
		q, err := o.quotes.GetQuote(ctx, order.QuoteId)
		if err != nil {
			log.Errorf("Can not find quote by the quete ID from order")
			return nil, err
		}
		err = o.updateQuoteVolumeValueandFee(ctx, order.Volume, order.Value, order.Fee, q, "COMPLETE")
		if err != nil {
			log.Errorf("Can not updateQuoteVolumeandValue ")
			return nil, err
		}

	}

	//Expired
	if in.Status == pb.OtcOrder_EXPIRED {
		if order.Status != pb.OtcOrder_UNPAID {
			err := status.Errorf(codes.PermissionDenied, "Status of %v cannot be expired.", order.Status)
			log.Error(err)
			return nil, err
		}
		err = o.expireOrder(ctx, order, eventId)
		if err != nil {
			log.Error(err)
			return nil, err
		}

		//update quote volume and value
		q, err := o.quotes.GetQuote(ctx, order.QuoteId)
		if err != nil {
			log.Errorf("Can not find quote by the quete ID from order")
			return nil, err
		}
		err = o.updateQuoteVolumeValueandFee(ctx, order.Volume, order.Value, order.Fee, q, "EXPIRE")
		if err != nil {
			log.Errorf("Fail to update quote volume and value when expire order: %s", err.Error())
			return nil, err
		}
	}

	err = o.trades.UpdateOtcOrderStatus(ctx, in.OrderId, eventId, in.Status)
	if err != nil {
		log.Println("Failed to update order status:" + in.OrderId.String())
		return nil, err
	}

	log.Info("update order status success")
	out = &pb.UpdateOtcOrderStatusResponse{
		Message: "success",
	}
	return
}

func (o OtcServer) DoUpdateOtcOrderRoomId(ctx context.Context, in *pb.UpdateOtcOrderRoomIdRequest) (out *pb.UpdateOtcOrderRoomIdResponse, err error) {
	err = o.trades.UpdateOtcOrderChatroomId(ctx, in.OrderId, in.ChatroomId)
	if err != nil {
		log.Println("Failed to update order status:" + in.OrderId.String())
		return
	}
	out = &pb.UpdateOtcOrderRoomIdResponse{
		Message: "success",
	}
	return
}

//Bid from a ASK Quote
func (o OtcServer) DoBuyQuote(ctx context.Context, in *pb.BuyQuoteRequest) (*pb.BuyQuoteResponse, error) {
	//Volume Value QuoteId MemberId
	//Price validate
	//if aud/usd lock balance

	q, err := o.quotes.GetQuote(ctx, in.QuoteId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "failed to find requested quote id: %v", exutil.UUIDtoA(in.QuoteId))
	}
	if q.Side != pb.OrderSide_ASK {
		err = fmt.Errorf("quote is not on sell side: ")
		return nil, err
	}

	if q.Status != pb.Quote_ON {
		err = fmt.Errorf("Quote is not on shelf now :%s", in.QuoteId.String())
		return nil, err
	}
	//validate value
	value, err := fl(in.Value)
	if err != nil {
		err = fmt.Errorf("invalid value detected")
		return nil, err
	}
	ret, err := o.validateOtcValue(value, q)
	if !ret {
		return nil, status.Errorf(codes.FailedPrecondition, "Invalid otc value: %v", err)
	}
	//validate volume
	volume, err := fl(in.Volume)
	if err != nil {
		err = fmt.Errorf("invalid volume detected")
		return nil, err
	}
	retVo, err := o.validateOtcVolume(volume, q)
	if !retVo {
		return nil, status.Errorf(codes.FailedPrecondition, "Invalid otc volume %v", err)
	}

	//price
	price := quo(value, volume)
	price64f, _ := price.Float64()

	//validate price
	imf := new(exutil.ImprFloat)
	baseDec, quoteDec := int(q.Instrument.Base.Decimal), int(q.Instrument.Quote.Decimal)
	qPrice := imf.FromFloat(q.Price).Shift(baseDec - quoteDec).ToFloat()
	oPrice := imf.FromFloat(price64f).Shift(baseDec - quoteDec).ToFloat()

	if qPrice-oPrice > 0.01 || qPrice-oPrice < -0.01 {
		err = status.Errorf(codes.FailedPrecondition, "Invalid price: Quote Price: %f, Order Price: %f", qPrice, oPrice)
		return nil, err
	}

	//expire time
	liveTime := q.ExpireBy
	if q.ExpireBy == 0 {
		err = status.Errorf(codes.Internal, "Quote's expire time should not be 0, Quote Id : %s", exutil.UUIDtoA(q.Id))
		return nil, err
	}
	createdTime := time.Now()
	createdTimeUnixNano := createdTime.UnixNano()
	expiredTime := createdTime.Add(time.Duration(liveTime) * time.Second).UnixNano()

	//fee
	rate, err := o.getOtcFeeRate(ctx, q.Owner)
	if err != nil {
		return nil, err
	}
	totalVolume := quo(volume, new(big.Float).Sub(big.NewFloat(1), rate))
	feeFloat := new(big.Float).Sub(totalVolume, volume)
	fee := feeFloat.Text('f', 0)

	//if buy all remained volume, fee = q lockedfee
	qvolume, err := fl(q.Volume)
	if err != nil {
		err = fmt.Errorf("invalid quote volume detected")
		return nil, err
	}
	if qvolume.Cmp(volume) == 0 {
		fee = q.LockedFee
	}

	otcO := &pb.OtcOrder{
		OrderNumber: "",
		Side:        pb.OrderSide_BID,
		MemberId:    in.MemberId,
		QuoteOwner:  q.Owner,
		QuoteId:     in.QuoteId,
		Price:       price64f,
		Volume:      in.Volume,
		Value:       in.Value,
		Status:      pb.OtcOrder_UNPAID,
		Time:        createdTimeUnixNano,
		Instrument:  q.Instrument,
		Fee:         fee,
		ExpiredTime: expiredTime,
	}

	//lock balance
	eventId := exutil.NewUUID()
	if _, ok := externalCurrency[q.Instrument.Quote.Symbol]; !ok {
		val, err := exutil.DecodeBigInt(in.Value)
		lr := &api.LockBalance{
			FromAmount: big.NewInt(0),
			ToAmount:   val,
			MemberId:   in.MemberId,
			CoinId:     q.GetInstrument().GetQuote().GetId(),
			ActivityId: eventId,
			Source:     pb.ActivitySource_ORDER,
		}

		err = o.apis.LockAccountBalance(ctx, lr)
		if err != nil {
			log.Errorf("failed to lock account balance %v", err)
			return nil, err
		}
	}
	//add pending
	account, err := o.findOrderAccount(ctx, otcO, q.Instrument.GetBase().GetId())
	if err != nil {
		return nil, err
	}
	err = o.addPending(ctx, account.GetId(), eventId, in.Volume)
	if err != nil {
		log.Errorf("Buy quote add pending error: %v", err)
		return nil, err
	}

	//Update Quote volume and value
	err = o.updateQuoteVolumeValueandFee(ctx, in.Volume, in.Value, fee, q, "CREATE")
	if err != nil {
		return nil, err
	}
	//TODO: using transaction
	id, err := o.trades.CreateOtcOrder(ctx, otcO, eventId)
	if err != nil {
		return nil, err
	}
	out := &pb.BuyQuoteResponse{
		OrderId: id,
	}
	return out, err
}

func (o OtcServer) DoSellQuote(ctx context.Context, in *pb.SellQuoteRequest) (out *pb.SellQuoteResponse, err error) {
	q, err := o.quotes.GetQuote(ctx, in.QuoteId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "failed to find requested quote id: %v", exutil.UUIDtoA(in.QuoteId))
	}
	if q.Side != pb.OrderSide_BID {
		err = fmt.Errorf("quote is not on buy side: ")
		return nil, err
	}

	if q.Status != pb.Quote_ON {
		err = fmt.Errorf("Quote is not on shelf now :%s", in.QuoteId.String())
		return
	}
	value, err := fl(in.Value)
	if err != nil {
		err = fmt.Errorf("invalid value detected")
		return
	}
	ret, err := o.validateOtcValue(value, q)
	if !ret {
		return nil, status.Errorf(codes.FailedPrecondition, "Invalid otc value: %v", err)
	}

	//validate volume
	volume, err := fl(in.Volume)
	if err != nil {
		err = fmt.Errorf("invalid volume detected")
		return nil, err
	}
	retVo, err := o.validateOtcVolume(volume, q)
	if !retVo {
		return nil, status.Errorf(codes.FailedPrecondition, "Invalid otc volume %v", err)
	}

	price := quo(value, volume)
	price64f, _ := price.Float64()
	//validate Price
	imf := new(exutil.ImprFloat)
	baseDec, quoteDec := int(q.Instrument.Base.Decimal), int(q.Instrument.Quote.Decimal)
	qPrice := imf.FromFloat(q.Price).Shift(baseDec - quoteDec).ToFloat()
	oPrice := imf.FromFloat(price64f).Shift(baseDec - quoteDec).ToFloat()

	if qPrice-oPrice > 0.01 || qPrice-oPrice < -0.01 {
		err = status.Errorf(codes.FailedPrecondition, "Invalid price: %f", price64f)
		return nil, err
	}
	//fee
	rate, err := o.getOtcFeeRate(ctx, q.Owner)
	if err != nil {
		return nil, err
	}
	volFloat, err := exutil.Float(in.Volume)
	if err != nil {
		return nil, err
	}
	feeFloat := mul(rate, volFloat)
	fee := feeFloat.Text('f', 0)
	vol, err := exutil.DecodeBigInt(in.Volume)
	if err != nil {
		return nil, err
	}

	//expire time
	liveTime := q.ExpireBy
	if q.ExpireBy == 0 {
		err = status.Errorf(codes.Internal, "Quote's expire time should not be 0, Quote Id : %s", exutil.UUIDtoA(q.Id))
		return nil, err
	}
	createdTime := time.Now()
	createdTimeUnixNano := createdTime.UnixNano()
	expiredTime := createdTime.Add(time.Duration(liveTime) * time.Second).UnixNano()

	otcO := &pb.OtcOrder{
		OrderNumber: "",
		Side:        pb.OrderSide_ASK,
		MemberId:    in.MemberId,
		QuoteId:     in.QuoteId,
		QuoteOwner:  q.Owner,
		Price:       price64f,
		Volume:      in.Volume,
		Value:       in.Value,
		Status:      pb.OtcOrder_UNPAID,
		Time:        createdTimeUnixNano,
		Instrument:  q.Instrument,
		Fee:         fee,
		ExpiredTime: expiredTime,
	}

	eventId := exutil.NewUUID()
	lr := &api.LockBalance{
		FromAmount: big.NewInt(0),
		ToAmount:   vol,
		MemberId:   in.MemberId,
		CoinId:     q.GetInstrument().GetBase().GetId(),
		ActivityId: eventId,
		Source:     pb.ActivitySource_ORDER,
	}

	err = o.apis.LockAccountBalance(ctx, lr)
	if err != nil {
		log.Errorf("failed to lock account balance %v", err)
		return
	}
	//add pending
	account, err := o.findQuoteAccount(ctx, q, q.Instrument.GetBase().GetId())
	if err != nil {
		return nil, err
	}
	err = o.addPending(ctx, account.GetId(), eventId, in.Volume)
	if err != nil {
		log.Errorf("Sell quote add pending error: %v", err)
		return nil, err
	}

	//Update Quote volume and value
	err = o.updateQuoteVolumeValueandFee(ctx, in.Volume, in.Value, "0", q, "CREATE")
	if err != nil {
		return
	}
	//create order
	id, err := o.trades.CreateOtcOrder(ctx, otcO, eventId)
	if err != nil {
		log.Error("Failed to create otc order for quote: " + in.QuoteId.String())
		return
	}

	out = &pb.SellQuoteResponse{
		OrderId: id,
	}
	return
}

//Internal functions to support RPC functions

func (o OtcServer) getOtcFeeRate(ctx context.Context, memberId *pb.UUID) (otcFeeRate *big.Float, err error) {
	member, err := o.apis.FindMember(ctx, memberId)
	if err != nil {
		return big.NewFloat(feeRate), err
	}
	if member.GetOtcFeeRate() != "" {
		rateFloat, err := strconv.ParseFloat(member.GetOtcFeeRate(), 64)
		if err != nil {
			return big.NewFloat(feeRate), err
		}
		otcFeeRate = big.NewFloat(rateFloat)
	} else {
		otcFeeRate = big.NewFloat(feeRate)
	}

	return
}

//Check if the value is in the range of the minValue and maxValue
func (o OtcServer) validateOtcValue(val *big.Float, q *pb.Quote) (ret bool, err error) {
	minVal, err := fl(q.MinValue)
	if err != nil {
		log.Errorf("invalid min value to big float:%v", err)
		return
	}

	maxVal, err := fl(q.MaxValue)
	if err != nil {
		log.Errorf("Can not transfer quote max value to big float:%v", err)
		return
		//maxVal = big.NewFloat(math.MaxFloat32)
	}
	qVal, err := fl(q.Value)
	if err != nil {
		log.Errorf("Can not transfer quote value to big float:%v", err)
		return
		//qVal = big.NewFloat(math.MaxFloat32)
	}
	if val.Cmp(minVal) < 0 || val.Cmp(maxVal) > 0 {
		err = fmt.Errorf("invalid	 value which is out of the required quote's value range: %s to %s", q.MinValue, q.MaxValue)
		return
	}
	if val.Cmp(qVal) > 0 {
		err = fmt.Errorf("order value: %d is larger than the quote's remained value %s", val, q.Value)
		return
	}
	ret = true
	return
}

//Check if order's volume is less than the quote remained volume
func (o OtcServer) validateOtcVolume(vol *big.Float, q *pb.Quote) (ret bool, err error) {
	qVol, err := fl(q.Volume)
	if err != nil {
		//qVol = big.NewFloat(math.MaxFloat32)
		return
	}
	if vol.Cmp(qVol) > 0 {
		err = fmt.Errorf("order volume: %v is larger than the quote's remained volume %s", vol, q.Volume)
		return
	}
	ret = true
	return
}

//Sub Quote volume value and fee when there is some action of a otc order action includes: CANCEL,CREATE,COMPLETE,EXPIRE
func (o OtcServer) updateQuoteVolumeValueandFee(ctx context.Context, orderVolume, orderValue, orderFee string, q *pb.Quote, orderAction string) (err error) {
	orderVolumeInt, ok := new(big.Int).SetString(orderVolume, 10)
	if !ok {
		return fmt.Errorf("can not transfer orderVolume to int %s", orderVolume)
	}
	orderValueInt, ok := new(big.Int).SetString(orderValue, 10)
	if !ok {
		return fmt.Errorf("can not transfer orderValue to int %s", orderValue)
	}
	orderFeeInt, ok := new(big.Int).SetString(orderFee, 10)
	if !ok {
		return fmt.Errorf("can not transfer orderFee to int %s", orderFee)
	}
	qVolume, ok := new(big.Int).SetString(q.Volume, 10)
	if !ok {
		return fmt.Errorf("can not transfer quoteVolume to int %s", q.Volume)
	}
	qValue, ok := new(big.Int).SetString(q.Value, 10)
	if !ok {
		return fmt.Errorf("can not transfer quoteValue to int %s", q.Value)
	}
	qProcessingVolume, ok := new(big.Int).SetString(q.ProcessingVolume, 10)
	if !ok {
		return fmt.Errorf("can not transfer quoteProcessingVolume to int %s", q.ProcessingVolume)
	}
	qLockedFee, ok := new(big.Int).SetString(q.LockedFee, 10)
	if !ok {
		return fmt.Errorf("can not transfer quote LockedFee to int %s", q.LockedFee)
	}

	var updatedVolume, updatedValue, updatedProcessingVolume string
	fields := bson.M{}
	updatedFee := ""

	switch orderAction {
	case "CANCEL", "EXPIRE":
		updatedVolume = new(big.Int).Add(qVolume, orderVolumeInt).String()
		updatedValue = new(big.Int).Add(qValue, orderValueInt).String()
		updatedProcessingVolume = new(big.Int).Sub(qProcessingVolume, orderVolumeInt).String()
		if q.Side == pb.OrderSide_ASK {
			updatedFee = new(big.Int).Add(qLockedFee, orderFeeInt).String()
		}
	case "CREATE":
		updatedVolume = new(big.Int).Sub(qVolume, orderVolumeInt).String()
		updatedValue = new(big.Int).Sub(qValue, orderValueInt).String()
		updatedProcessingVolume = new(big.Int).Add(qProcessingVolume, orderVolumeInt).String()
		if q.Side == pb.OrderSide_ASK {
			updatedFee = new(big.Int).Sub(qLockedFee, orderFeeInt).String()
		}
	case "COMPLETE":
		updatedVolume = q.Volume
		updatedValue = q.Value
		updatedProcessingVolume = new(big.Int).Sub(qProcessingVolume, orderVolumeInt).String()
	}
	fields = bson.M{"value": updatedValue, "volume": updatedVolume, "processingVolume": updatedProcessingVolume}

	if orderAction == "COMPLETE" && updatedVolume == "0" {
		fields["status"] = pb.Quote_OFF
	}
	if updatedFee != "" {
		fields["lockedFee"] = updatedFee
	}

	return o.quotes.UpdateQuote(ctx, q.Id, fields)

}

func (o OtcServer) payOrder(ctx context.Context, order *pb.OtcOrder, eventId *pb.UUID) (err error) {
	//if NOT CNY
	//Bid Quote Currency Account --> Ask Quote Currency Account
	if _, ok := externalCurrency[order.Instrument.Quote.Symbol]; !ok {
		var fromAccountID, toAccountID *pb.UUID
		//ASK ORDER: SELL COIN recieve quote currency
		//BID ORDER: BUY COIN pay money
		orderAccount, err := o.findOrderAccount(ctx, order, order.Instrument.Quote.Id)
		if err != nil {
			log.Errorf("Invalid order ID")
			return err
		}

		q, err := o.quotes.GetQuote(ctx, order.QuoteId)
		if err != nil {
			log.Errorf("Can not find quote by the quete ID from order")
			return err
		}
		quoteAccount, err := o.findQuoteAccount(ctx, q, q.Instrument.Quote.Id)
		if err != nil {
			log.Errorf("Can not find quote account %v", q.Id)
			return err
		}

		if order.Side == pb.OrderSide_ASK {
			fromAccountID = quoteAccount.Id
			toAccountID = orderAccount.Id
		} else {
			fromAccountID = orderAccount.Id
			toAccountID = quoteAccount.Id
		}
		req := &pb.ReleaseLockedBalanceRequest{
			From:   fromAccountID,
			To:     toAccountID,
			Amount: order.Value,
			Order: &pb.OrderRef{
				Id: order.Id,
			},
			Event: &pb.OrderEvent{
				Id: eventId,
			},
		}
		err = o.apis.ReleaselockedBalance(ctx, req)
		if err != nil {
			log.Error("fail to release locked value")
			return err
		}
	}
	return
}

func (o OtcServer) releaseCoin(ctx context.Context, order *pb.OtcOrder, eventId *pb.UUID) (err error) {
	//ASK Base Currency Account --> BID Base Currency Account

	var fromAccountID, toAccountID *pb.UUID
	//ASK ORDER: SELL COIN / send coin to quote owner
	//BID ORDER: BUY COIN / recieve coin from quote owner
	orderAccount, err := o.findOrderAccount(ctx, order, order.Instrument.Base.Id)
	if err != nil {
		log.Errorf("Invalid order ID")
		return err
	}

	q, err := o.quotes.GetQuote(ctx, order.QuoteId)
	if err != nil {
		log.Errorf("Can not find quote by the quete ID from order")
		return err
	}
	quoteAccount, err := o.findQuoteAccount(ctx, q, q.Instrument.Base.Id)
	if err != nil {
		log.Errorf("Can not find quote account %v", q.Id)
		return err
	}
	var transferAmount string
	orderFee, ok := new(big.Int).SetString(order.Fee, 10)
	if !ok {
		return fmt.Errorf("can not transfer order fee to int %s", q.Volume)
	}
	orderVolume, ok := new(big.Int).SetString(order.Volume, 10)
	if !ok {
		return fmt.Errorf("can not transfer order volume to int %s", q.Volume)
	}
	if order.Side == pb.OrderSide_ASK {
		fromAccountID = orderAccount.Id
		toAccountID = quoteAccount.Id
		//fee
		transferAmount = new(big.Int).Sub(orderVolume, orderFee).String()
	} else {
		fromAccountID = quoteAccount.Id
		toAccountID = orderAccount.Id
		transferAmount = order.Volume
	}

	req := &pb.ReleaseLockedBalanceRequest{
		From:   fromAccountID,
		To:     toAccountID,
		Amount: transferAmount,
		Order: &pb.OrderRef{
			Id: order.Id,
		},
		Event: &pb.OrderEvent{
			Id: eventId,
		},
	}
	err = o.apis.ReleaselockedBalance(ctx, req)
	if err != nil {
		log.Error("fail to release locked value")
		return
	}
	if orderFee.Int64() != 0 {
		req := &pb.ReleaseLockedBalanceRequest{
			From:   fromAccountID,
			To:     nil,
			Amount: order.Fee,
			Order: &pb.OrderRef{
				Id: order.Id,
			},
			Event: &pb.OrderEvent{
				Id: exutil.NewUUID(),
			},
		}
		err = o.apis.ReleaselockedBalance(ctx, req)
		if err != nil {
			log.Error("fail to release fee")
			return
		}
	}

	return
}

func (o OtcServer) expireOrder(ctx context.Context, order *pb.OtcOrder, eventId *pb.UUID) (err error) {
	//Release locked account of the for Expirement
	if _, ok := externalCurrency[order.Instrument.Quote.Symbol]; !(ok && order.Side == pb.OrderSide_BID) {
		var coinId *pb.UUID
		var amount string
		if order.Side == pb.OrderSide_ASK {
			coinId = order.GetInstrument().GetBase().Id
			amount = order.Volume
		} else {
			coinId = order.GetInstrument().GetQuote().Id
			amount = order.Value
		}

		orderAccount, err := o.findOrderAccount(ctx, order, coinId)
		if err != nil {
			log.Errorf(err.Error())
			return err
		}
		req := &pb.ReleaseLockedBalanceRequest{
			From:   orderAccount.Id,
			To:     orderAccount.Id,
			Amount: amount,
			Order: &pb.OrderRef{
				Id: order.Id,
			},
			Event: &pb.OrderEvent{
				Id: eventId,
			},
		}
		err = o.apis.ReleaselockedBalance(ctx, req)
		if err != nil {
			log.Error("fail to release locked balance: %s", err.Error())
			return err
		}
	}
	//release pending
	err = o.releasePendingPro(ctx, order)
	if err != nil {
		log.Errorf("release pending error: %v", err)
		return
	}

	return
}

func (o OtcServer) refundOrder(ctx context.Context, order *pb.OtcOrder, eventId *pb.UUID) (err error) {
	if _, ok := externalCurrency[order.Instrument.Quote.Symbol]; ok {
		return
	}

	//GetAccount
	orderAccount, err := o.findOrderAccount(ctx, order, order.Instrument.Quote.Id)
	if err != nil {
		log.Errorf("Invalid order ID")
		return err
	}

	q, err := o.quotes.GetQuote(ctx, order.QuoteId)
	if err != nil {
		log.Errorf("Can not find quote by the quete ID from order")
		return err
	}
	quoteAccount, err := o.findQuoteAccount(ctx, q, q.Instrument.Quote.Id)
	if err != nil {
		log.Errorf("Can not find quote account %v", q.Id)
		return err
	}

	var fromAccountID, toAccountID, fromMemberID, toMemberID *pb.UUID
	if order.Side == pb.OrderSide_ASK {
		fromAccountID = orderAccount.Id
		fromMemberID = order.MemberId
		toAccountID = quoteAccount.Id
		toMemberID = q.Owner
	} else {
		fromAccountID = quoteAccount.Id
		fromMemberID = q.Owner
		toAccountID = orderAccount.Id
		toMemberID = order.MemberId
	}

	//lock
	val, err := exutil.DecodeBigInt(order.Value)
	lr := &api.LockBalance{
		FromAmount: big.NewInt(0),
		ToAmount:   val,
		MemberId:   fromMemberID,
		CoinId:     order.GetInstrument().GetQuote().GetId(),
		ActivityId: eventId,
		Source:     pb.ActivitySource_ORDER,
	}

	err = o.apis.LockAccountBalance(ctx, lr)
	if err != nil {
		log.Errorf("failed to lock account balance %v", err)
		return err
	}

	//release: transfer back
	req := &pb.ReleaseLockedBalanceRequest{
		From:   fromAccountID,
		To:     toAccountID,
		Amount: val.Text(10),
		Order: &pb.OrderRef{
			Id: order.Id,
		},
		Event: &pb.OrderEvent{
			Id: eventId,
		},
	}
	err = o.apis.ReleaselockedBalance(ctx, req)
	if err != nil {
		log.Error("fail to release locked value")
		return
	}
	//lock: lock back
	lbr := &api.LockBalance{
		FromAmount: big.NewInt(0),
		ToAmount:   val,
		MemberId:   toMemberID,
		CoinId:     order.GetInstrument().GetQuote().GetId(),
		ActivityId: eventId,
		Source:     pb.ActivitySource_ORDER,
	}

	err = o.apis.LockAccountBalance(ctx, lbr)
	if err != nil {
		log.Errorf("failed to lock account balance %v", err)
		return err
	}

	return
}

func (o OtcServer) addPending(ctx context.Context, accountId, orderId *pb.UUID, amount string) (err error) {
	_, err = o.apis.AddPending(ctx, &pb.AddPendingRequest{
		AccountId: accountId,
		OrderId:   orderId,
		Amount:    amount,
	})
	return
}

func (o OtcServer) releasePending(ctx context.Context, accountId, orderId *pb.UUID, amount string) (err error) {
	_, err = o.apis.ReleasePending(ctx, &pb.ReleasePendingRequest{
		AccountId: accountId,
		OrderId:   orderId,
		Amount:    amount,
	})
	return
}

func (o OtcServer) releasePendingPro(ctx context.Context, order *pb.OtcOrder) (err error) {
	eventId := exutil.NewUUID()
	var account *pb.AccountDefined
	if order.Side == pb.OrderSide_ASK {
		//if order is on selling side , release buyer's pending -- quote owner's pending
		var q *pb.Quote
		q, err = o.quotes.GetQuote(ctx, order.QuoteId)
		if err != nil {
			return status.Errorf(codes.NotFound, "failed to find requested quote id: %v", exutil.UUIDtoA(order.QuoteId))
		}
		account, err = o.findQuoteAccount(ctx, q, order.Instrument.GetBase().GetId())
		if err != nil {
			return
		}
	} else {
		account, err = o.findOrderAccount(ctx, order, order.Instrument.GetBase().GetId())
		if err != nil {
			return
		}
	}
	err = o.releasePending(ctx, account.GetId(), eventId, order.Volume)
	if err != nil {
		log.Errorf("Release pending error: %v", err)
		return err
	}
	return
}
