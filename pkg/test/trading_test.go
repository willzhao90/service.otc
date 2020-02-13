package test

import (
	"context"
	"testing"

	log "github.com/sirupsen/logrus"
	"gitlab.com/sdce/exlib/exutil"
	exmongo "gitlab.com/sdce/exlib/mongo"
	pb "gitlab.com/sdce/protogo"
	"gitlab.com/sdce/service/otc/pkg/repository"
	"gotest.tools/assert"
)

func TestOrderCreate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db := exmongo.Connect(ctx, exmongo.Config{
		URI:    "mongodb://localhost:27017",
		DbName: "test",
	})
	defer db.Close(ctx)
	qid, _ = exutil.AtoUUID("5c8467223c1b73fc9ba602d6")
	trRepo := repository.NewOtcTradeRepository(db)
	user1, _ = exutil.AtoUUID("5c7bc423a22a98e52b5ac1e4")
	order := &pb.OtcOrder{
		Side:      pb.OrderSide_BID,
		MemberId:  user1,
		QuoteId:   qid,
		Method:    pb.PaymentMethod_BANK,
		Price:     5000.0,
		MaxLimit:  "1000000000",
		MinLimit:  "10",
		Country:   "CN",
		TimeLimit: 60000,
		Volume:    "1000000000",
		Value:     "5000000000000",
		Status:    pb.OtcOrder_UNPAID,
	}

	oid, err := trRepo.CreateOtcOrder(ctx, order)
	if err != nil {
		log.Errorf("failed to create otc order : %v", err)
		t.Fail()
	}

	io, err := trRepo.GetOtcOrder(ctx, oid)

	if err != nil {
		log.Errorf("failed to get otc order : %v", err)
		t.Fail()
	}

	assert.Assert(t, io.Volume == "1000000000", "correct volume expected from get order ")
	assert.Assert(t, io.Value == "5000000000000", "correct value expected from get order ")

	db.Db.Drop(ctx)
}

func TestOrderUpdate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db := exmongo.Connect(ctx, exmongo.Config{
		URI:    "mongodb://localhost:27017",
		DbName: "test",
	})
	defer db.Close(ctx)
	qid, _ = exutil.AtoUUID("5c8467223c1b73fc9ba602d6")
	trRepo := repository.NewOtcTradeRepository(db)
	user1, _ = exutil.AtoUUID("5c7bc423a22a98e52b5ac1e4")
	order := &pb.OtcOrder{
		Side:      pb.OrderSide_BID,
		MemberId:  user1,
		QuoteId:   qid,
		Method:    pb.PaymentMethod_BANK,
		Price:     5000.0,
		MaxLimit:  "1000000000",
		MinLimit:  "10",
		Country:   "CN",
		TimeLimit: 60000,
		Volume:    "1000000000",
		Value:     "5000000000000",
		Status:    pb.OtcOrder_UNPAID,
		Events:    []*pb.OrderEvent{},
	}

	oid, err := trRepo.CreateOtcOrder(ctx, order)
	if err != nil {
		log.Errorf("failed to create otc order : %v", err)
		t.Fail()
	}

	err = trRepo.UpdateOtcOrder(ctx, oid, "2000000000", "10000000000000")
	if err != nil {
		log.Errorf("failed to update otc order %v", err)
		t.Fail()
	}
	f := &repository.OrderFilter{
		MemberId: user1,
	}

	updated, err := trRepo.SearchOtcOrders(ctx, f)
	if err != nil {
		log.Errorf("failed to search otc orders %v", err)
		t.Fail()
	}
	assert.Assert(t, len(updated) == 1, "only one order should be returned")
	assert.Assert(t, updated[0].Volume == "2000000000", "volume of updated order should be 2000000000")
	assert.Assert(t, updated[0].Value == "10000000000000", "volume of updated order should be 2000000000")

	err = trRepo.DeleteOtcOrder(ctx, oid)
	if err != nil {
		log.Errorf("failed to delete otc order %v", err)
		t.Fail()
	}
	f2 := &repository.OrderFilter{
		MemberId: user1,
	}
	deleted, err := trRepo.SearchOtcOrders(ctx, f2)
	if err != nil {
		log.Errorf("failed to search otc orders %v", err)
		t.Fail()
	}
	assert.Assert(t, len(deleted) == 1, "1 order should be returned")
	assert.Assert(t, deleted[0].Status == pb.OtcOrder_CANCELLED, "otc order has been cancelled.")
	db.Db.Drop(ctx)
}
