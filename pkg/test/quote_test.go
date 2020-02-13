package test

import (
	"context"
	"testing"

	"gotest.tools/assert"

	log "github.com/sirupsen/logrus"
	exmongo "gitlab.com/sdce/exlib/mongo"
	pb "gitlab.com/sdce/protogo"
	"gitlab.com/sdce/service/otc/pkg/repository"
	"go.mongodb.org/mongo-driver/bson"
)

func TestQuoteUpdate(t *testing.T) {
	q := &pb.Quote{
		Instrument: FakeInstrumentRef,
		Price:      0.001,
		Side:       pb.OrderSide_ASK,
		Owner:      user1,
		Type:       pb.Quote_REGULAR,
		Volume:     "100000000",
		Value:      "100000",
		Events:     []*pb.OrderEvent{},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db := exmongo.Connect(ctx, exmongo.Config{
		URI:    "mongodb://localhost:27017",
		DbName: "test",
	})
	defer db.Close(ctx)
	quoteRepo := repository.NewQuoteRepo(db)

	qid, err := quoteRepo.CreateQuote(ctx, q)
	if err != nil {
		log.Errorf("failed to create quote: %v", err)
		t.Fail()
	}

	quoteToUpdate, err := quoteRepo.GetQuote(ctx, qid)
	if err != nil {
		log.Errorf("failed to get quote: %v", err)
		t.Fail()
	}

	assert.Assert(t, quoteToUpdate.Value == "100000", "quote value is still 100000")
	assert.Assert(t, quoteToUpdate.Volume == "100000000", "quote volume is still 100000000")

	fields := bson.M{"volume": "200000000", "value": "200000"}
	err = quoteRepo.UpdateQuote(ctx, qid, fields)
	if err != nil {
		log.Errorf("failed to update quote %v", err)
		t.Fail()
	}

	quoteUpdated, err := quoteRepo.GetQuote(ctx, qid)
	if err != nil {
		log.Errorf("failed to get quote: %v", err)
		t.Fail()
	}
	assert.Assert(t, quoteUpdated.Value == "200000", "quote value is still 100000")
	assert.Assert(t, quoteUpdated.Volume == "200000000", "quote volume is still 100000000")
	db.Db.Drop(ctx)
}

func TestQuoteCreateSearch(t *testing.T) {
	quotes := []*pb.Quote{
		&pb.Quote{
			Instrument: FakeInstrumentRef,
			Price:      0.001,
			Side:       pb.OrderSide_ASK,
			Owner:      user1,
			Type:       pb.Quote_REGULAR,
			Volume:     "100000000",
			Value:      "100000",
		},
		&pb.Quote{
			Instrument: FakeInstrumentRef,
			Price:      0.001,
			Side:       pb.OrderSide_BID,
			Owner:      user2,
			Type:       pb.Quote_REGULAR,
			Volume:     "200000000",
			Value:      "200000",
		},
		&pb.Quote{
			Instrument: FakeInstrumentRef,
			Price:      0.001,
			Side:       pb.OrderSide_ASK,
			Owner:      user3,
			Type:       pb.Quote_REGULAR,
			Volume:     "300000000",
			Value:      "300000",
		},
		&pb.Quote{
			Instrument: FakeInstrumentRef,
			Price:      0.001,
			Side:       pb.OrderSide_BID,
			Owner:      user4,
			Type:       pb.Quote_REGULAR,
			Volume:     "400000000",
			Value:      "400000",
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db := exmongo.Connect(ctx, exmongo.Config{
		URI:    "mongodb://localhost:27017",
		DbName: "test",
	})
	defer db.Close(ctx)
	quoteRepo := repository.NewQuoteRepo(db)
	for _, q := range quotes {
		_, err := quoteRepo.CreateQuote(ctx, q)
		if err != nil {
			log.Errorf("failed to create quote: %v", err)
			t.Fail()
		}
	}

	filter := &repository.QuoteFilter{
		MemberId:     user1,
		BaseCurrency: FakeInstrumentRef.GetBase().GetId(),
	}
	out, err := quoteRepo.SearchQuotes(ctx, filter)
	if err != nil {
		t.Fail()
	}
	for _, q := range out {
		log.Infof("quote %v", q)
	}

	assert.Assert(t, len(out) == 1, "only one record should be returned")
	db.Db.Drop(ctx)
}
