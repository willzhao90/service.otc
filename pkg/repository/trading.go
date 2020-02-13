package repository

import (
	"context"
	"fmt"
	"time"

	"gitlab.com/sdce/exlib/exutil"
	exmongo "gitlab.com/sdce/exlib/mongo"
	pb "gitlab.com/sdce/protogo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	OtcOrder = "otc_order"
)

type OrderFilter struct {
	MemberId      *pb.UUID
	Status        []pb.OtcOrder_OrderStatus
	Side          pb.OrderSide
	BaseCurrency  string
	QuoteCurrency string
	PageIdx       int64
	PageSize      int64
}

type OtcTradeRepository interface {
	CreateOtcOrder(ctx context.Context, data *pb.OtcOrder, eventId *pb.UUID) (*pb.UUID, error)
	SearchOtcOrders(ctx context.Context, filter *OrderFilter) (out []*pb.OtcOrder, count int64, err error)
	GetOtcOrder(ctx context.Context, id *pb.UUID) (*pb.OtcOrder, error)
	UpdateOtcOrder(ctx context.Context, id *pb.UUID, volume, value string) error
	UpdateOtcOrderStatus(ctx context.Context, id, eventId *pb.UUID, status pb.OtcOrder_OrderStatus) error
	UpdateOtcOrderChatroomId(ctx context.Context, id *pb.UUID, roomId string) error
	DeleteOtcOrder(ctx context.Context, id *pb.UUID) error
	SearchExpiredOtcOrders(ctx context.Context) (out []*pb.OtcOrder, err error)
}

type otcTradeRepoMongo struct {
	DB *mongo.Collection
}

//NewOtcTradeRepository returns a quote repository instance backed by MongoDB
func NewOtcTradeRepository(db *exmongo.Database) OtcTradeRepository {
	return &otcTradeRepoMongo{DB: db.CreateCollection(OtcOrder)}
}

func (o *otcTradeRepoMongo) CreateOtcOrder(ctx context.Context, data *pb.OtcOrder, eventId *pb.UUID) (*pb.UUID, error) {
	if data.Id == nil {
		data.Id = exutil.NewUUID()
	}
	if data.Method == pb.PaymentMethod_INVALID_METHOD {
		data.Method = pb.PaymentMethod_BANK
	}
	if eventId == nil {
		eventId = exutil.NewUUID()
	}
	data.Events = []*pb.OrderEvent{&pb.OrderEvent{
		Type:             pb.OrderEventType_CREATE_ORDER,
		Price:            data.Price,
		UpdateFromVolume: "0",
		UpdateFromValue:  "0",
		UpdateToVolume:   data.Volume,
		UpdateToValue:    data.Value,
		Time:             time.Now().UnixNano(),
	}}
	res, err := o.DB.InsertOne(ctx, data)
	insertedID := res.InsertedID.(primitive.ObjectID)
	if err != nil {
		return nil, err
	}

	return &pb.UUID{Bytes: insertedID[:]}, nil
}

func (o *otcTradeRepoMongo) SearchOtcOrders(ctx context.Context, filter *OrderFilter) (out []*pb.OtcOrder, count int64, err error) {
	opts := &options.FindOptions{}
	if filter.PageSize > 0 {
		opts = exmongo.NewPaginationOptions(filter.PageIdx, filter.PageSize)
	}
	opts.SetSort(bson.M{"time": -1}) // Time descending order
	fobj := bson.M{}
	if filter.MemberId != nil {
		fobj["$or"] = bson.A{
			bson.M{"memberId": filter.MemberId},
			bson.M{"quoteowner": filter.MemberId},
		}
	}
	if len(filter.Status) != 0 {
		fobj["status"] = bson.M{"$in": filter.Status}
	}

	if filter.Side != pb.OrderSide_ORDER_SIDE_INVALID {
		fobj["side"] = filter.Side
	}
	if filter.BaseCurrency != "" {
		fobj["instrument.base.symbol"] = filter.BaseCurrency
	}

	if filter.QuoteCurrency != "" {
		fobj["instrument.quote.symbol"] = filter.QuoteCurrency
	}

	cur, err := o.DB.Find(ctx, fobj, opts)
	if err != nil {
		return nil, 0, err
	}
	defer cur.Close(ctx)
	count, err = o.DB.CountDocuments(ctx, fobj)
	if err != nil {
		return nil, 0, err
	}
	err = exmongo.DecodeCursorToSlice(ctx, cur, &out)
	return
}

func (o *otcTradeRepoMongo) GetOtcOrder(ctx context.Context, id *pb.UUID) (*pb.OtcOrder, error) {
	var out pb.OtcOrder
	err := o.DB.FindOne(ctx, exmongo.IDFilter(id)).Decode(&out)
	return &out, err
}

func (o *otcTradeRepoMongo) UpdateOtcOrderStatus(ctx context.Context, id, eventId *pb.UUID, status pb.OtcOrder_OrderStatus) (err error) {
	res := o.DB.FindOne(ctx, exmongo.IDFilter(id))
	if res.Err() != nil {
		err = res.Err()
		return
	}
	order := &pb.OtcOrder{}
	err = res.Decode(order)
	if err != nil {
		return
	}
	event := pb.OrderEvent{
		Id:   eventId,
		Type: pb.OrderEventType_UPDATE_ORDER,
		Time: time.Now().UnixNano(),
	}

	if (order.Status == pb.OtcOrder_CANCELLED || order.Status == pb.OtcOrder_RESOLVED) ||
		(order.Status == pb.OtcOrder_UNPAID && status != pb.OtcOrder_PAID && status != pb.OtcOrder_CANCELLED && status != pb.OtcOrder_APPEAL && status != pb.OtcOrder_EXPIRED) ||
		(order.Status == pb.OtcOrder_PAID && status != pb.OtcOrder_APPEAL && status != pb.OtcOrder_COMPLETED && status != pb.OtcOrder_UNPAID) ||
		(order.Status == pb.OtcOrder_APPEAL && status != pb.OtcOrder_RESOLVED && status != pb.OtcOrder_CANCELLED) {
		err = fmt.Errorf("order transition invalid: %s to %s", order.Status.String(), status.String())
		return
	}
	mOb := bson.M{"status": status}
	mOb["lastUpdatedTime"] = time.Now().UnixNano()
	if status == pb.OtcOrder_COMPLETED {
		mOb["releasedTime"] = time.Now().UnixNano()
	}

	_, err = o.DB.UpdateOne(ctx, exmongo.IDFilter(id),
		bson.M{
			"$set":  mOb,
			"$push": bson.M{"events": event},
		},
	)
	return
}

func (o *otcTradeRepoMongo) UpdateOtcOrderChatroomId(ctx context.Context, id *pb.UUID, roomId string) (err error) {
	res := o.DB.FindOne(ctx, exmongo.IDFilter(id))
	if res.Err() != nil {
		err = res.Err()
		return
	}
	order := &pb.OtcOrder{}
	err = res.Decode(order)
	if err != nil {
		return
	}

	_, err = o.DB.UpdateOne(ctx, exmongo.IDFilter(id),
		bson.M{
			"$set": bson.M{"chatroomId": roomId},
		},
	)
	return
}

func (o *otcTradeRepoMongo) UpdateOtcOrder(ctx context.Context, id *pb.UUID, volume, value string) error {
	valNum, err := exutil.Float(value)
	if err != nil {
		return fmt.Errorf("cannot decode value %v to float", value)
	}
	volNum, err := exutil.Float(volume)
	if err != nil {
		return fmt.Errorf("cannot decode value %v to float", volume)
	}

	price, _ := valNum.Quo(valNum, volNum).Float64()

	event := pb.OrderEvent{
		Type:           pb.OrderEventType_UPDATE_ORDER,
		UpdateToVolume: volume,
		UpdateToValue:  value,
		Price:          price,
		Time:           time.Now().UnixNano(),
	}

	fields := bson.M{
		"$set":  bson.M{"volume": volume, "value": value, "price": price},
		"$push": bson.M{"events": event},
	}
	_, err = o.DB.UpdateOne(ctx, exmongo.IDFilter(id), fields)
	return err
}

func (o *otcTradeRepoMongo) DeleteOtcOrder(ctx context.Context, id *pb.UUID) error {
	event := pb.OrderEvent{
		Type: pb.OrderEventType_CANCEL_ORDER,
		Time: time.Now().UnixNano(),
	}

	_, err := o.DB.UpdateOne(ctx, exmongo.IDFilter(id),
		bson.M{
			"$set": bson.M{"status": pb.OtcOrder_CANCELLED},
			"$push": bson.M{
				"events": event}})

	return err
}

func (o *otcTradeRepoMongo) SearchExpiredOtcOrders(ctx context.Context) (out []*pb.OtcOrder, err error) {
	timeofNow := time.Now().UnixNano()
	fobj := bson.D{
		{Key: "expiredTime", Value: bson.M{"$lte": timeofNow}},
		{Key: "status", Value: pb.OtcOrder_UNPAID},
	}
	cur, err := o.DB.Find(ctx, fobj, nil)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	err = exmongo.DecodeCursorToSlice(ctx, cur, &out)
	return
}
