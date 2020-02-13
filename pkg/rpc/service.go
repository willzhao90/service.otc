package rpc

import (
	exmongo "gitlab.com/sdce/exlib/mongo"
	"gitlab.com/sdce/service/otc/pkg/api"
	"gitlab.com/sdce/service/otc/pkg/repository"
)

const (
	// TopicsNewOrderPrefix is the prefix of Kafka "new-order" topics.
	TopicsNewOrderPrefix = "new-otc-"
)

// OtcServer instance
type OtcServer struct {
	quotes          repository.QuoteRepository
	trades          repository.OtcTradeRepository
	currencyorders  repository.CurrencyOrderRepository
	merchants       repository.MerchantRepository
	merchantMargins repository.MerchantMarginRepository

	apis api.Api
}

const (
	port = ":8030"
)

//StartRPCServer starts RPC server

func NewOtcTradingServer(api api.Api, db *exmongo.Database) *OtcServer {
	return &OtcServer{
		apis:            api,
		quotes:          repository.NewQuoteRepo(db),
		trades:          repository.NewOtcTradeRepository(db),
		currencyorders:  repository.NewCurrencyOrderRepo(db),
		merchants:       repository.NewMerchantRepo(db),
		merchantMargins: repository.NewMerchantMarginRepo(db),
	}
}
