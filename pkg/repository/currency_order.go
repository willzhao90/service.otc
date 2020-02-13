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
	currencyOrderCollectionName = "currency_order"
)

type CurrencyOrderFilter struct {
	Merchant       *pb.UUID
	OwnerName      string
	Status         []pb.CurrencyOrder_Status
	Side           pb.CurrencyOrder_Side
	Ticker         string
	PageIdx        int64
	PageSize       int64
	OwnerId        *pb.UUID
	FromTime       int64
	ToTime         int64
	OwnerWalletUID int32
}
type CurrencyOrderRepository interface {
	CreateCurrencyOrder(ctx context.Context, data *pb.CurrencyOrder) (*pb.UUID, error)
	GetCurrencyOrder(ctx context.Context, id *pb.UUID) (*pb.CurrencyOrder, error)
	UpdateCurrencyOrder(ctx context.Context, currencyOrder *pb.CurrencyOrder) (err error)
	SearchCurrencyOrders(ctx context.Context, filter *CurrencyOrderFilter) (out []*pb.CurrencyOrder, count int64, err error)
	UpdateExpiredCurrencyOrders(ctx context.Context) (err error)
}

type currencyOrderRepoMongo struct {
	DB *mongo.Collection
}

func NewCurrencyOrderRepo(db *exmongo.Database) CurrencyOrderRepository {
	db.CreateCollection(currencyOrderCollectionName)
	return &currencyOrderRepoMongo{DB: db.CreateCollection(currencyOrderCollectionName)}
}

func (m *currencyOrderRepoMongo) CreateCurrencyOrder(ctx context.Context, data *pb.CurrencyOrder) (*pb.UUID, error) {
	data.Id = exutil.NewUUID()
	res, err := m.DB.InsertOne(ctx, data)
	if err != nil {
		return nil, err
	}
	insertedID := res.InsertedID.(primitive.ObjectID)

	return &pb.UUID{Bytes: insertedID[:]}, nil
}

func (m *currencyOrderRepoMongo) GetCurrencyOrder(ctx context.Context, id *pb.UUID) (*pb.CurrencyOrder, error) {
	var out pb.CurrencyOrder
	err := m.DB.FindOne(ctx, exmongo.IDFilter(id)).Decode(&out)
	return &out, err
}

func (m *currencyOrderRepoMongo) UpdateCurrencyOrder(ctx context.Context, currencyOrder *pb.CurrencyOrder) (err error) {
	currencyOrder.UpdatedAt = time.Now().UnixNano()
	_, err = m.DB.UpdateOne(ctx, exmongo.IDFilter(currencyOrder.Id), bson.M{"$set": currencyOrder})
	return
}

func (m *currencyOrderRepoMongo) SearchCurrencyOrders(ctx context.Context, filter *CurrencyOrderFilter) (out []*pb.CurrencyOrder, count int64, err error) {
	opts := &options.FindOptions{}
	if filter.PageSize > 0 {
		opts = exmongo.NewPaginationOptions(filter.PageIdx, filter.PageSize)
	}
	opts.SetSort(bson.M{"createdAt": -1})
	fobj := bson.M{}
	if filter.Merchant != nil {
		fobj["merchant"] = filter.Merchant
	}
	if len(filter.Status) != 0 {
		fobj["status"] = bson.M{"$in": filter.Status}
	}
	if filter.OwnerName != "" {
		fobj["owner.name"] = filter.OwnerName
	}
	if filter.Side != pb.CurrencyOrder_SIDE_INVALID {
		fobj["side"] = filter.Side
	}
	if filter.Ticker != "" {
		fobj["ticker"] = filter.Ticker
	}
	if filter.OwnerId != nil {
		fobj["owner.id"] = filter.OwnerId
	}
	if filter.OwnerWalletUID != 0 {
		fobj["owner.walletUID"] = filter.OwnerWalletUID
	}

	if filter.FromTime != 0 && filter.ToTime != 0 {
		fobj["updatedAt"] = TimeRangeFilter(filter.FromTime, filter.ToTime)
	}

	cur, err := m.DB.Find(ctx, fobj, opts)
	if err != nil {
		return nil, 0, err
	}
	count, err = m.DB.CountDocuments(ctx, fobj)
	if err != nil {
		return nil, 0, err
	}
	err = exmongo.DecodeCursorToSlice(ctx, cur, &out)
	return
}

func (m *currencyOrderRepoMongo) UpdateExpiredCurrencyOrders(ctx context.Context) (err error) {
	timeofNow := time.Now().UnixNano()
	fobj := bson.D{
		{Key: "expiredTime", Value: bson.M{"$lte": timeofNow}},
		{Key: "status", Value: bson.M{"$in": bson.A{pb.CurrencyOrder_INITIATED, pb.CurrencyOrder_OPEN}}},
	}
	result, err := m.DB.UpdateMany(ctx, fobj, bson.M{"$set": bson.M{"status": pb.CurrencyOrder_EXPIRED, "updatedAt": timeofNow}})
	if err != nil {
		log.Error(err)
		return
	}
	log.Infof("There are %d currency orders expired, there are %d currency orders updated", result.MatchedCount, result.ModifiedCount)
	return
}
