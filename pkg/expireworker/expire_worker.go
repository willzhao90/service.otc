package expireworker

import (
	"context"
	"fmt"

	"github.com/robfig/cron"
	log "github.com/sirupsen/logrus"
	"gitlab.com/sdce/exlib/exutil"
	"gitlab.com/sdce/exlib/mongo"
	pb "gitlab.com/sdce/protogo"
	"gitlab.com/sdce/service/otc/pkg/otcapi"
	"gitlab.com/sdce/service/otc/pkg/repository"
)

type ExpireCheckService interface {
}

type expireCheckManager struct {
	trades         repository.OtcTradeRepository
	currencyOrders repository.CurrencyOrderRepository
	otcApis        otcapi.OTCApi
}

func NewExpireCheckService(otcApi otcapi.OTCApi, db *mongo.Database) *expireCheckManager {
	return &expireCheckManager{
		trades:         repository.NewOtcTradeRepository(db),
		currencyOrders: repository.NewCurrencyOrderRepo(db),
		otcApis:        otcApi,
	}
}

func (ecm *expireCheckManager) Run(ctx context.Context) error {
	c := cron.New()
	c.AddFunc("@every 1m", func() {
		//for currency order
		log.Info("This is from the expiration check cron job every minute.")
		err := ecm.currencyOrders.UpdateExpiredCurrencyOrders(ctx)
		if err != nil {
			log.Errorf("Fail to update expired currency orders: %v", err)
		}
		//for otc order
		err = ecm.updateExpiredOtcOrder(ctx)
		if err != nil {
			log.Errorf("Fail to expire otc order! : %v", err)
		}
	})
	c.Start()
	<-ctx.Done()
	c.Stop()
	return fmt.Errorf("Cron job of expiration check stopped unexpectedly.")
}

func (ecm *expireCheckManager) updateExpiredOtcOrder(ctx context.Context) (err error) {
	expiredOrders, err := ecm.trades.SearchExpiredOtcOrders(ctx)
	if err != nil {
		log.Errorf("Search expired otc order err: %v", err)
		return
	}
	log.Infof("There are %d expired otc orders found.", len(expiredOrders))
	if len(expiredOrders) == 0 {
		return nil
	}
	for _, order := range expiredOrders {
		req := &pb.UpdateOtcOrderStatusRequest{
			OrderId: order.GetId(),
			Status:  pb.OtcOrder_EXPIRED,
		}
		err = ecm.otcApis.UpdateOrder(ctx, req)
		if err != nil {
			log.Errorf("Update expired otc order err: %v orderId: %s", err, exutil.UUIDtoA(order.Id))
			return
		}
		log.Infof("Expired order: %s updated", exutil.UUIDtoA(order.Id))
	}
	return
}
