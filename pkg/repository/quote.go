package repository

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"

	"gitlab.com/sdce/exlib/exutil"
	exmongo "gitlab.com/sdce/exlib/mongo"
	pb "gitlab.com/sdce/protogo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	CollectionName = "quote"
)

type QuoteFilter struct {
	MemberId      *pb.UUID
	Status        pb.Quote_QuoteStatus
	Side          pb.OrderSide
	BaseCurrency  string
	QuoteCurrency string
	PageIdx       int64
	PageSize      int64
}

type QuoteRepository interface {
	CreateQuote(ctx context.Context, data *pb.Quote) (*pb.UUID, error)
	SearchQuotes(ctx context.Context, filter *QuoteFilter) (out []*pb.Quote, count int64, err error)
	GetQuote(ctx context.Context, id *pb.UUID) (*pb.Quote, error)
	UpdateQuote(ctx context.Context, id *pb.UUID, fields bson.M) error
	DeleteQuote(ctx context.Context, id, eventId *pb.UUID) error
	CreateSDCEQuote(ctx context.Context, ticker string, buyUnitPrice *pb.UnitPrice, sellUnitPrice *pb.UnitPrice) error
	SearchSDCEQuote(ctx context.Context, ticker string) (out *pb.CurrencyQuote, err error)
}

const (
	QuoteCollection     = "quote"
	SDCEQuoteCollection = "sdce_quote"
)

// NewQuoteRepo returns a quote repository instance backed by MongoDB
func NewQuoteRepo(db *exmongo.Database) QuoteRepository {
	c := db.CreateCollection(SDCEQuoteCollection)
	_, err := c.Indexes().CreateOne(context.Background(), mongo.IndexModel{
		Keys:    bson.D{{"ticker", 1}},
		Options: new(options.IndexOptions).SetUnique(true),
	})
	if err != nil {
		log.Fatalf("Create index error %v", err)
	}
	return &quoteMongoRepo{
		Quote:     db.CreateCollection(QuoteCollection),
		SdceQuote: c,
	}
}

type quoteMongoRepo struct {
	Quote     *mongo.Collection
	SdceQuote *mongo.Collection
}

func (m *quoteMongoRepo) CreateQuote(ctx context.Context, data *pb.Quote) (*pb.UUID, error) {
	data.Id = exutil.NewUUID()
	res, err := m.Quote.InsertOne(ctx, data)
	if err != nil {
		return nil, err
	}
	insertedID := res.InsertedID.(primitive.ObjectID)
	return &pb.UUID{Bytes: insertedID[:]}, nil
}

func (m *quoteMongoRepo) SearchQuotes(ctx context.Context, filter *QuoteFilter) (out []*pb.Quote, count int64, err error) {
	opts := &options.FindOptions{}
	if filter.PageSize > 0 {
		opts = exmongo.NewPaginationOptions(filter.PageIdx, filter.PageSize)
	}

	fobj := bson.M{}

	if filter.MemberId != nil {
		fobj["owner"] = filter.MemberId
	}
	fobj["status"] = filter.Status
	if filter.Side != pb.OrderSide_ORDER_SIDE_INVALID {
		fobj["side"] = filter.Side
		if filter.Side == pb.OrderSide_ASK {
			opts.SetSort(bson.M{"price": 1})
		} else {
			opts.SetSort(bson.M{"price": -1})
		}
	}
	if filter.BaseCurrency != "" {
		fobj["instrument.base.symbol"] = filter.BaseCurrency
	}

	if filter.QuoteCurrency != "" {
		fobj["instrument.quote.symbol"] = filter.QuoteCurrency
	}
	cur, err := m.Quote.Find(ctx, fobj, opts)
	if err != nil {
		return nil, 0, err
	}
	defer cur.Close(ctx)
	count, err = m.Quote.CountDocuments(ctx, fobj)
	if err != nil {
		return nil, 0, err
	}
	err = exmongo.DecodeCursorToSlice(ctx, cur, &out)
	//
	return
}

func (m *quoteMongoRepo) GetQuote(ctx context.Context, id *pb.UUID) (*pb.Quote, error) {
	var out pb.Quote
	err := m.Quote.FindOne(ctx, exmongo.IDFilter(id)).Decode(&out)
	return &out, err
}

func (m *quoteMongoRepo) UpdateQuote(ctx context.Context, id *pb.UUID, fields bson.M) (err error) {

	event := pb.OrderEvent{
		Type: pb.OrderEventType_UPDATE_ORDER,
		Time: time.Now().UnixNano(),
	}

	mobj := bson.M{}
	_, got := fields["volume"]
	if got {
		event.UpdateToVolume = fields["volume"].(string)
	}

	_, got = fields["price"]
	if got {
		event.Price = fields["price"].(float64)
	}

	_, got = fields["value"]
	if got {
		event.UpdateToValue = fields["value"].(string)
	}
	mobj["$set"] = fields
	if got {
		mobj["$push"] = bson.M{
			"events": event,
		}
	}
	_, err = m.Quote.UpdateOne(ctx, exmongo.IDFilter(id), mobj)
	return err
}

func (m *quoteMongoRepo) DeleteQuote(ctx context.Context, id, eventId *pb.UUID) error {
	event := pb.OrderEvent{
		Id:   eventId,
		Type: pb.OrderEventType_CANCEL_ORDER,
		Time: time.Now().UnixNano(),
	}

	_, err := m.Quote.UpdateOne(ctx, exmongo.IDFilter(id),
		bson.M{
			"$set": bson.M{"status": pb.Quote_CLOSED},
			"$push": bson.M{
				"events": event}})

	return err
}

func (m *quoteMongoRepo) CreateSDCEQuote(ctx context.Context, ticker string, buyUnitPrice *pb.UnitPrice, sellUnitPrice *pb.UnitPrice) error {
	var fObj = bson.M{}
	tm := time.Now().UnixNano()
	if buyUnitPrice != nil {
		fObj["unitPrice.price"] = buyUnitPrice.Price
		fObj["unitPrice.updatedAt"] = tm
		if buyUnitPrice.GetCurrency() != nil {
			fObj["unitPrice.currency"] = buyUnitPrice.GetCurrency()
		}
	}
	if sellUnitPrice != nil {
		fObj["sellUnitPrice.price"] = sellUnitPrice.Price
		fObj["sellUnitPrice.updatedAt"] = tm
		if sellUnitPrice.GetCurrency() != nil {
			fObj["sellUnitPrice.currency"] = sellUnitPrice.GetCurrency()
		}
	}
	_, err := m.SdceQuote.UpdateOne(ctx, bson.M{"ticker": ticker},
		bson.M{
			"$set": fObj,
		},
		options.Update().SetUpsert(true),
	)
	return err
}

func (m *quoteMongoRepo) SearchSDCEQuote(ctx context.Context, ticker string) (out *pb.CurrencyQuote, err error) {
	filter := bson.M{}
	if ticker != "" {
		filter["ticker"] = ticker
	}
	err = m.SdceQuote.FindOne(ctx, filter).Decode(&out)
	return
}
