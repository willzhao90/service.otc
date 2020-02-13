package rpc

import (
	log "github.com/sirupsen/logrus"
	"gitlab.com/sdce/exlib/exutil"
	exmongo "gitlab.com/sdce/exlib/mongo"
	pb "gitlab.com/sdce/protogo"
	"gitlab.com/sdce/service/otc/pkg/repository"
	"golang.org/x/net/context"
)

func (o OtcServer) DoGetMerchantMargin(ctx context.Context, in *pb.GetMerchantMarginRequest) (*pb.GetMerchantMarginResponse, error) {
	merchantMargin, err := o.merchantMargins.GetMerchantMargin(ctx, in.MerchantMarginId)
	if err != nil {
		log.Errorf("Failed to get merchant margin order: %v", err)
		err = exmongo.ErrorToRpcError(err)
		return nil, err
	}
	res := &pb.GetMerchantMarginResponse{
		MerchantMargin: merchantMargin,
	}
	return res, err
}

func (o OtcServer) DoUpsertMerchantMargin(ctx context.Context, in *pb.UpsertMerchantMarginRequest) (out *pb.UpsertMerchantMarginResponse, err error) {
	filter := &repository.MerchantMarginFilter{
		MerchantId: in.MerchantMargin.Merchant,
		Side:       in.MerchantMargin.Side,
		Ticker:     in.MerchantMargin.Ticker,
	}

	merchantMargins, count, err := o.merchantMargins.SearchMerchantMargins(ctx, filter)
	if err != nil {
		log.Errorf("search merchant margin err when upsert margin: &v", err)
	}
	if count != 1 || err != nil {
		log.Info("New merchant margin id")
		in.MerchantMargin.Id = exutil.NewUUID()
	} else {
		log.Info("Find existed merchant margin id")
		in.MerchantMargin.Id = merchantMargins[0].Id
	}

	err = o.merchantMargins.UpsertMerchantMargin(ctx, in.GetMerchantMargin())
	if err != nil {
		log.Errorf("upsert merchant margin error: %v", err)
		return nil, exmongo.ErrorToRpcError(err)
	}
	out = &pb.UpsertMerchantMarginResponse{
		Id: in.GetMerchantMargin().GetId(),
	}
	return
}

func (o OtcServer) DoSearchMerchantMargins(ctx context.Context, in *pb.SearchMerchantMarginsRequest) (out *pb.SearchMerchantMarginsResponse, err error) {
	filter := &repository.MerchantMarginFilter{
		MerchantId: in.MerchantId,
		Side:       in.Side,
		Ticker:     in.Ticker,
		Name:       in.Name,
	}

	merchantMarginss, count, err := o.merchantMargins.SearchMerchantMargins(ctx, filter)
	if err != nil {
		log.Errorf("search merchant margin err: &v", err)
		return nil, exmongo.ErrorToRpcError(err)
	}

	out = &pb.SearchMerchantMarginsResponse{
		MerchantMargins: merchantMarginss,
		ResultCount:     count,
	}
	return
}
