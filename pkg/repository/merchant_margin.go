package repository

import (
	"context"
	"log"

	exmongo "gitlab.com/sdce/exlib/mongo"
	pb "gitlab.com/sdce/protogo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	merchantMarginCollectionName = "merchant_margin"
)

type MerchantMarginFilter struct {
	MerchantId *pb.UUID
	Side       pb.MerchantMargin_Side
	Ticker     string
	Name       string
}
type MerchantMarginRepository interface {
	GetMerchantMargin(ctx context.Context, id *pb.UUID) (*pb.MerchantMargin, error)
	UpsertMerchantMargin(ctx context.Context, merchantMargin *pb.MerchantMargin) (err error)
	SearchMerchantMargins(ctx context.Context, filter *MerchantMarginFilter) (out []*pb.MerchantMargin, count int64, err error)
}

type merchantMarginRepoMongo struct {
	DB *mongo.Collection
}

func NewMerchantMarginRepo(db *exmongo.Database) MerchantMarginRepository {
	db.CreateCollection(merchantMarginCollectionName)

	unique := true
	merchantMarginCollection := db.Db.Collection(merchantMarginCollectionName)
	_, err := merchantMarginCollection.Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		mongo.IndexModel{
			Keys:    bson.D{{"merchant", 1}, {"ticker", 1}, {"side", 1}},
			Options: &options.IndexOptions{Unique: &unique},
		},
		mongo.IndexModel{
			Keys: bson.D{{"name", 1}},
		},
	})
	if err != nil {
		log.Fatal(err.Error())
		return nil
	}
	return &merchantMarginRepoMongo{DB: merchantMarginCollection}
}

func (m *merchantMarginRepoMongo) GetMerchantMargin(ctx context.Context, id *pb.UUID) (*pb.MerchantMargin, error) {
	var out pb.MerchantMargin
	err := m.DB.FindOne(ctx, exmongo.IDFilter(id)).Decode(&out)
	return &out, err
}

func (m *merchantMarginRepoMongo) UpsertMerchantMargin(ctx context.Context, merchantMargin *pb.MerchantMargin) (err error) {
	_, err = m.DB.UpdateOne(ctx, exmongo.IDFilter(merchantMargin.Id),
		bson.M{
			"$set": merchantMargin,
		},
		options.Update().SetUpsert(true),
	)
	return
}

func (m *merchantMarginRepoMongo) SearchMerchantMargins(ctx context.Context, filter *MerchantMarginFilter) (out []*pb.MerchantMargin, count int64, err error) {
	fobj := bson.M{}
	if filter.MerchantId != nil {
		fobj["merchant"] = filter.MerchantId
	}
	if filter.Side != pb.MerchantMargin_SIDE_INVALID {
		fobj["side"] = filter.Side
	}
	if filter.Ticker != "" {
		fobj["ticker"] = filter.Ticker
	}
	if filter.Name != "" {
		fobj["name"] = filter.Name
	}

	cur, err := m.DB.Find(ctx, fobj, nil)
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
