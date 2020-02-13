package test

import (
	"bytes"
	context "context"
	"testing"

	"github.com/golang/mock/gomock"
	log "github.com/sirupsen/logrus"
	"gitlab.com/sdce/exlib/exutil"
	exmongo "gitlab.com/sdce/exlib/mongo"
	pb "gitlab.com/sdce/protogo"
	"gitlab.com/sdce/service/otc/pkg/rpc"
	"gotest.tools/assert"
)

func TestCreateQuote(t *testing.T) {
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

	gr := &pb.GetQuoteDetailsRequest{
		QuoteId: created,
	}

	qd, err := rpcServer.DoGetQuoteDetails(ctx, gr)
	if err != nil {
		log.Errorf("failed to get quote")
		t.Fail()
	}
	assert.Assert(t, bytes.Equal(qd.GetQuote().GetId().Bytes, created.Bytes), "id equals")

	inReq := &pb.ListQuoteRequest{
		UserId:       user1,
		BaseCurrency: FakeInstrumentRef.Base.GetId(),
	}
	qres, err := rpcServer.DoListQuote(ctx, inReq)
	if err != nil {
		log.Errorf("failed to list quotes %v", err)
		t.Fail()
	}

	ql := qres.GetQuotes()
	assert.Assert(t, len(ql) == 1, "only 1 quote returned")
	log.Infof("quote returned %v", ql[0])
	db.Db.Drop(ctx)
}

func TestUpdateQuote(t *testing.T) {
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
