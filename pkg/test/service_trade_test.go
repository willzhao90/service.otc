package test

import (
	context "context"
	"testing"

	"github.com/golang/mock/gomock"
	log "github.com/sirupsen/logrus"
	"gitlab.com/sdce/exlib/exutil"
	exmongo "gitlab.com/sdce/exlib/mongo"
	pb "gitlab.com/sdce/protogo"
	"gitlab.com/sdce/service/otc/pkg/rpc"
)

func TestBuyOrder(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	api := NewMockApi(ctrl)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	user1, _ := exutil.AtoUUID("5c7e0420ae3e23c93982b684")
	coin, _ := exutil.AtoUUID("5c7f6bc09e7405297329f087")
	BTC, _ := exutil.AtoUUID("5c7cfe670948c6e942e3e6e1")
	BTCRef = &pb.CurrencyRef{
		Symbol:  "BTC",
		Decimal: 8,
		Id:      BTC,
		Type:    pb.Currency_CURRENCY_BTC,
	}
	db := exmongo.Connect(ctx, exmongo.Config{
		URI:    "mongodb://localhost:27017",
		DbName: "test",
	})
	defer db.Close(ctx)

	log.Errorf("testing using coin :%s", exutil.UUIDtoA(coin))

	q := &pb.Quote{
		Instrument:             FakeInstrumentRef,
		Price:                  0.001,
		Side:                   pb.OrderSide_ASK,
		Owner:                  user1,
		Type:                   pb.Quote_REGULAR,
		Volume:                 "100000000",
		Value:                  "100000",
		Events:                 []*pb.OrderEvent{},
		AcceptedPaymentMethods: []pb.PaymentMethod{pb.PaymentMethod_BANK},
	}

	api.EXPECT().LockAccountBalance(ctx, gomock.Any()).Return(nil)
	//	api.EXPECT().FindMemberAccount(ctx, user1, coin).Return(accounts, nil)
	rpcServer := rpc.NewOtcTradingServer(api, db)

	req := &pb.CreateQuoteRequest{
		Quote:  q,
		Method: []pb.PaymentMethod{pb.PaymentMethod_BANK},
	}
	res, err := rpcServer.DoCreateQuote(ctx, req)
	if err != nil {
		log.Errorf("failed to create quote: %v", err)
		t.Fail()
	}
	created := res.GetId()

	accId, _ := exutil.AtoUUID("5c7f6bc09e7405297329f087")
	// fakeAccAUD := &pb.AccountDefined{
	// 	Id:       accId,
	// 	OnHold:   "0",
	// 	Balance:  "100000000",
	// 	Owner:    user1,
	// 	Currency: AUDRef,
	// }

	bReq := &pb.BuyQuoteRequest{
		QuoteId:   created,
		MemberId:  user1,
		AccountId: accId,
		Method:    pb.PaymentMethod_BANK,
		Volume:    "50000000",
		Value:     "50000",
		MinVal:    "10000",
		MaxVal:    "50000",
		Country:   "AU",
		TimeLimit: 1000 * 60 * 30,
	}

	boRes, err := rpcServer.DoBuyQuote(ctx, bReq)
	if err != nil {
		log.Errorf("failed to buy quote %v", err)
		t.Fail()
	}
	log.Infof("newly created order %s", exutil.UUIDtoA(boRes.OrderId))
	db.Db.Drop(ctx)
}

func TestSellOrder(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	api := NewMockApi(ctrl)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	user1, _ := exutil.AtoUUID("5c7e0420ae3e23c93982b684")
	coin, _ := exutil.AtoUUID("5c7f6bc09e7405297329f087")
	BTC, _ := exutil.AtoUUID("5c7cfe670948c6e942e3e6e1")
	BTCRef = &pb.CurrencyRef{
		Symbol:  "BTC",
		Decimal: 8,
		Id:      BTC,
		Type:    pb.Currency_CURRENCY_BTC,
	}
	db := exmongo.Connect(ctx, exmongo.Config{
		URI:    "mongodb://localhost:27017",
		DbName: "test",
	})
	defer db.Close(ctx)

	log.Errorf("testing using coin :%s", exutil.UUIDtoA(coin))

	q := &pb.Quote{
		Instrument:             FakeInstrumentRef,
		Price:                  0.001,
		Side:                   pb.OrderSide_BID,
		Owner:                  user1,
		Type:                   pb.Quote_REGULAR,
		Volume:                 "100000000",
		Value:                  "100000",
		Events:                 []*pb.OrderEvent{},
		AcceptedPaymentMethods: []pb.PaymentMethod{pb.PaymentMethod_BANK},
	}

	api.EXPECT().LockAccountBalance(ctx, gomock.Any()).Return(nil)
	//	api.EXPECT().FindMemberAccount(ctx, user1, coin).Return(accounts, nil)
	rpcServer := rpc.NewOtcTradingServer(api, db)

	req := &pb.CreateQuoteRequest{
		Quote:  q,
		Method: []pb.PaymentMethod{pb.PaymentMethod_BANK},
	}
	res, err := rpcServer.DoCreateQuote(ctx, req)
	if err != nil {
		log.Errorf("failed to create quote: %v", err)
		t.Fail()
	}
	created := res.GetId()

	accId, _ := exutil.AtoUUID("5c7f6bc09e7405297329f087")

	bReq := &pb.SellQuoteRequest{
		QuoteId:         created,
		MemberId:        user1,
		AccountId:       accId,
		AcceptedMethods: []pb.PaymentMethod{pb.PaymentMethod_BANK},
		Volume:          "50000000",
		Value:           "50000",
		MinVal:          "10000",
		MaxVal:          "50000",
		Country:         "AU",
		TimeLimit:       1000 * 60 * 30,
	}
	api.EXPECT().LockAccountBalance(ctx, gomock.Any()).Return(nil)
	boRes, err := rpcServer.DoSellQuote(ctx, bReq)
	if err != nil {
		log.Errorf("failed to buy quote %v", err)
		t.Fail()
	}
	log.Infof("newly created order %s", exutil.UUIDtoA(boRes.OrderId))
	db.Db.Drop(ctx)
}

func TestUpdateOrder(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	api := NewMockApi(ctrl)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	user1, _ := exutil.AtoUUID("5c7e0420ae3e23c93982b684")
	coin, _ := exutil.AtoUUID("5c7f6bc09e7405297329f087")
	BTC, _ := exutil.AtoUUID("5c7cfe670948c6e942e3e6e1")
	BTCRef = &pb.CurrencyRef{
		Symbol:  "BTC",
		Decimal: 8,
		Id:      BTC,
		Type:    pb.Currency_CURRENCY_BTC,
	}
	db := exmongo.Connect(ctx, exmongo.Config{
		URI:    "mongodb://localhost:27017",
		DbName: "test",
	})
	defer db.Close(ctx)

	log.Errorf("testing using coin :%s", exutil.UUIDtoA(coin))

	q := &pb.Quote{
		Instrument:             FakeInstrumentRef,
		Price:                  0.001,
		Side:                   pb.OrderSide_ASK,
		Owner:                  user1,
		Type:                   pb.Quote_REGULAR,
		Volume:                 "100000000",
		Value:                  "100000",
		Events:                 []*pb.OrderEvent{},
		AcceptedPaymentMethods: []pb.PaymentMethod{pb.PaymentMethod_BANK},
	}

	api.EXPECT().LockAccountBalance(ctx, gomock.Any()).Return(nil)
	//	api.EXPECT().FindMemberAccount(ctx, user1, coin).Return(accounts, nil)
	rpcServer := rpc.NewOtcTradingServer(api, db)

	req := &pb.CreateQuoteRequest{
		Quote:  q,
		Method: []pb.PaymentMethod{pb.PaymentMethod_BANK},
	}
	res, err := rpcServer.DoCreateQuote(ctx, req)
	if err != nil {
		log.Errorf("failed to create quote: %v", err)
		t.Fail()
	}
	created := res.GetId()
	log.Printf("new created quote: %s", exutil.UUIDtoA(created))

	template := &pb.Quote{}
	fm, err := exutil.GenerateFieldMask([]string{"Volume", "Value"}, template)
	ur := &pb.UpdateQuoteRequest{
		NewQuote: &pb.Quote{
			Id:         created,
			Volume:     "200000000",
			Value:      "200000",
			Instrument: FakeInstrumentRef,
		},
		UpdateMask: fm,
	}

	uRes, err := rpcServer.DoUpdateQuote(ctx, ur)
	if err != nil {
		log.Errorf("failed to update quote %v", err)
		t.Fail()
	}

	log.Infof("updated quote: %v", uRes.Message)
	db.Db.Drop(ctx)
}

func TestUpdateOtcOrder(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	api := NewMockApi(ctrl)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	user1, _ := exutil.AtoUUID("5c7e0420ae3e23c93982b684")
	user2, _ := exutil.AtoUUID("5c7f72aaae9411b00e000491")
	coin, _ := exutil.AtoUUID("5c7f6bc09e7405297329f087")
	BTC, _ := exutil.AtoUUID("5c7cfe670948c6e942e3e6e1")
	BTCRef = &pb.CurrencyRef{
		Symbol:  "BTC",
		Decimal: 8,
		Id:      BTC,
		Type:    pb.Currency_CURRENCY_BTC,
	}
	db := exmongo.Connect(ctx, exmongo.Config{
		URI:    "mongodb://localhost:27017",
		DbName: "test",
	})
	defer db.Close(ctx)

	log.Errorf("testing using coin :%s", exutil.UUIDtoA(coin))

	q := &pb.Quote{
		Instrument:             FakeInstrumentRef,
		Price:                  0.001,
		Side:                   pb.OrderSide_ASK,
		Owner:                  user1,
		Type:                   pb.Quote_REGULAR,
		Volume:                 "100000000",
		Value:                  "100000",
		Events:                 []*pb.OrderEvent{},
		AcceptedPaymentMethods: []pb.PaymentMethod{pb.PaymentMethod_BANK},
	}

	rpcServer := rpc.NewOtcTradingServer(api, db)

	accId1, _ := exutil.AtoUUID("5c7f6bc09e7405297329f087")
	accId2, _ := exutil.AtoUUID("5c7cff810948c6e942e3e6e3")
	fakeAccBTC1 := &pb.AccountDefined{
		Id:       accId1,
		OnHold:   "0",
		Balance:  "100000000",
		Owner:    user1,
		Currency: BTCRef,
	}

	fakeAccBTC2 := &pb.AccountDefined{
		Id:       accId2,
		OnHold:   "0",
		Balance:  "100000000",
		Owner:    user1,
		Currency: BTCRef,
	}

	api.EXPECT().LockAccountBalance(ctx, gomock.Any()).Return(nil)
	api.EXPECT().FindMemberAccount(ctx, user1, gomock.Any()).Return([]*pb.AccountDefined{fakeAccBTC1}, nil)
	api.EXPECT().FindMemberAccount(ctx, user2, gomock.Any()).Return([]*pb.AccountDefined{fakeAccBTC2}, nil)
	api.EXPECT().ReleaselockedBalance(ctx, gomock.Any()).Return(nil)

	req := &pb.CreateQuoteRequest{
		Quote:  q,
		Method: []pb.PaymentMethod{pb.PaymentMethod_BANK},
	}
	res, err := rpcServer.DoCreateQuote(ctx, req)
	if err != nil {
		log.Errorf("failed to create quote: %v", err)
		t.Fail()
	}
	created := res.GetId()
	bReq := &pb.BuyQuoteRequest{
		QuoteId:   created,
		MemberId:  user2,
		AccountId: accId2,
		Method:    pb.PaymentMethod_BANK,
		Volume:    "50000000",
		Value:     "50000",
		MinVal:    "10000",
		MaxVal:    "50000",
		Country:   "AU",
		TimeLimit: 1000 * 60 * 30,
	}

	boRes, err := rpcServer.DoBuyQuote(ctx, bReq)
	if err != nil {
		log.Errorf("failed to buy quote %v", err)
		t.Fail()
	}
	log.Infof("newly created order %s", exutil.UUIDtoA(boRes.OrderId))

	uoReq := &pb.UpdateOtcOrderStatusRequest{
		OrderId:  boRes.OrderId,
		BuyerId:  user2,
		SellerId: user1,
		Status:   pb.OtcOrder_PAID,
	}

	uoRes, err := rpcServer.DoUpdateOrder(ctx, uoReq)
	if err != nil {
		log.Errorf("cannot update order %v", err)
		t.Fail()
	}

	log.Infof("updated order successed %v", uoRes.Message)
	db.Db.Drop(ctx)
}
