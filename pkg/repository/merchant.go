package repository

import (
	"context"

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
	merchantCollectionName = "merchant"
)

type MerchantFilter struct {
	ContactPerson *pb.UUID
	ClientId      string
	Name          string
	AdminClientId string
}

type MerchantRepository interface {
	CreateMerchant(ctx context.Context, data *pb.Merchant) (*pb.UUID, error)
	GetMerchant(ctx context.Context, id *pb.UUID) (*pb.Merchant, error)
	UpdateMerchant(ctx context.Context, id *pb.UUID, fields bson.M) (err error)
	SearchMerchant(ctx context.Context, filter *MerchantFilter) (out []*pb.Merchant, count int64, err error)
}

type merchantRepoMongo struct {
	DB *mongo.Collection
}

func NewMerchantRepo(db *exmongo.Database) MerchantRepository {
	db.CreateCollection(merchantCollectionName)
	unique := true
	merchantCollection := db.Db.Collection(merchantCollectionName)
	_, err := merchantCollection.Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys:    bson.D{{"name", 1}},
			Options: &options.IndexOptions{Unique: &unique},
		},
		{
			Keys:    bson.D{{"clientID", 1}},
			Options: &options.IndexOptions{Unique: &unique},
		},
	})
	if err != nil {
		log.Fatal(err.Error())
		return nil
	}
	return &merchantRepoMongo{DB: db.CreateCollection(merchantCollectionName)}
}

func (m *merchantRepoMongo) CreateMerchant(ctx context.Context, data *pb.Merchant) (*pb.UUID, error) {
	data.Id = exutil.NewUUID()
	res, err := m.DB.InsertOne(ctx, data)
	if err != nil {
		return nil, err
	}
	insertedID := res.InsertedID.(primitive.ObjectID)
	return &pb.UUID{Bytes: insertedID[:]}, nil
}

func (m *merchantRepoMongo) GetMerchant(ctx context.Context, id *pb.UUID) (*pb.Merchant, error) {
	var out pb.Merchant
	err := m.DB.FindOne(ctx, exmongo.IDFilter(id)).Decode(&out)
	return &out, err
}

func (m *merchantRepoMongo) UpdateMerchant(ctx context.Context, id *pb.UUID, fields bson.M) (err error) {
	_, err = m.DB.UpdateOne(ctx, exmongo.IDFilter(id), bson.M{"$set": fields})
	return
}

func (m *merchantRepoMongo) SearchMerchant(ctx context.Context, filter *MerchantFilter) (out []*pb.Merchant, count int64, err error) {
	fobj := bson.M{}
	if filter.ContactPerson != nil {
		fobj["contactPerson"] = filter.ContactPerson
	}
	if filter.ClientId != "" {
		fobj["clientID"] = filter.ClientId
	}

	if filter.Name != "" {
		fobj["name"] = filter.Name
	}
	if filter.AdminClientId != "" {
		fobj["adminClientID"] = filter.AdminClientId
	}
	log.Infof("Search Merchant: %v", fobj)
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
