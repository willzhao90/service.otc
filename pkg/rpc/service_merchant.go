package rpc

import (
	log "github.com/sirupsen/logrus"
	"gitlab.com/sdce/exlib/exutil"
	exmongo "gitlab.com/sdce/exlib/mongo"
	pb "gitlab.com/sdce/protogo"
	"gitlab.com/sdce/service/otc/pkg/repository"
	"golang.org/x/net/context"
)

func (o OtcServer) DoCreateMerchant(ctx context.Context, in *pb.CreateMerchantRequest) (*pb.CreateMerchantResponse, error) {
	mID, err := o.merchants.CreateMerchant(ctx, in.Merchant)
	if err != nil {
		log.Errorf("Failed to create merchant: &v", err)
		return nil, exmongo.ErrorToRpcError(err)
	}

	res := &pb.CreateMerchantResponse{
		Id: mID,
	}
	return res, err
}

func (o OtcServer) DoGetMerchant(ctx context.Context, in *pb.GetMerchantRequest) (*pb.GetMerchantResponse, error) {
	merchant, err := o.merchants.GetMerchant(ctx, in.MerchantId)
	if err != nil {
		log.Errorf("Failed to get merchant order: &v", err)
		err = exmongo.ErrorToRpcError(err)
		return nil, err
	}
	res := &pb.GetMerchantResponse{
		Merchant: merchant,
	}
	return res, err
}

func (o OtcServer) DoUpdateMerchant(ctx context.Context, in *pb.UpdateMerchantRequest) (out *pb.UpdateMerchantResponse, err error) {

	uobj, err := exutil.ApplyFieldMaskToBson(in.GetMerchant(), in.GetUpdateMask())
	if err != nil {
		log.Errorf("Failed to ApplyFieldMaskToBson for merchant")
		return
	}

	err = o.merchants.UpdateMerchant(ctx, in.GetMerchant().GetId(), uobj)
	if err != nil {
		return nil, exmongo.ErrorToRpcError(err)
	}

	out = &pb.UpdateMerchantResponse{
		Id: in.GetMerchant().GetId(),
	}
	return
}

func (o OtcServer) DoSearchMerchants(ctx context.Context, in *pb.SearchMerchantsRequest) (out *pb.SearchMerchantsResponse, err error) {
	filter := &repository.MerchantFilter{
		ClientId:      in.ClientId,
		ContactPerson: in.ContactPerson,
		Name:          in.Name,
		AdminClientId: in.AdminClientId,
	}

	merchants, count, err := o.merchants.SearchMerchant(ctx, filter)
	if err != nil {
		log.Errorf("search merchant err: &v", err)
		return nil, exmongo.ErrorToRpcError(err)
	}

	for _, merchant := range merchants {
		merchant.Info = nil
	}

	out = &pb.SearchMerchantsResponse{
		Merchants:   merchants,
		ResultCount: count,
	}
	return
}

func (o OtcServer) DoSearchMerchantsInfo(ctx context.Context, in *pb.SearchMerchantsInfoRequest) (out *pb.SearchMerchantsInfoResponse, err error) {
	filter := &repository.MerchantFilter{
		ClientId:      in.ClientId,
		ContactPerson: in.ContactPerson,
		Name:          in.Name,
	}

	merchants, _, err := o.merchants.SearchMerchant(ctx, filter)
	if err != nil {
		log.Errorf("search merchant err: &v", err)
		return nil, exmongo.ErrorToRpcError(err)
	}

	infos := []*pb.MerchantInfo{}
	for _, merchant := range merchants {
		if merchant.GetInfo() != nil {
			infos = append(infos, merchant.GetInfo())
		}
	}

	out = &pb.SearchMerchantsInfoResponse{
		Infos:       infos,
		ResultCount: int64(len(infos)),
	}
	return
}
