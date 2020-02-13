package main

/**
* this tool is used for convert paymentDetail(struct) to payFundDetail(json string)
* before use, you need to config yaml as below:
*
mongo:
  uri: mongodb://root:Welcome2sdce0329#@dds-j0bea362990645441180-pub.mongodb.australia.rds.aliyuncs.com:3717/admin
  dbName: otc
*/
import (
	"context"
	"encoding/json"
	"fmt"

	"gitlab.com/sdce/exlib/exutil"

	log "github.com/sirupsen/logrus"

	"gitlab.com/sdce/exlib/config"
	"gitlab.com/sdce/exlib/mongo"
	"gitlab.com/sdce/service/otc/pkg/repository"
)

func getConfig() (mongoConf *mongo.Config, err error) {
	v, err := config.LoadConfig("service.otc")
	err = v.ReadInConfig()
	if err != nil {
		return
	}

	mongoConf, err = mongo.GetConfig(v)
	if err != nil {
		return
	}
	return
}

func main() {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgoConf, err := getConfig()
	if err != nil {
		log.Error(err)
	}
	db := mongo.Connect(ctx, *mgoConf)
	defer db.Close(ctx)
	repo := repository.NewCurrencyOrderRepo(db)
	var pageIndex int64 = 0
	for {
		out, _, err := repo.SearchCurrencyOrders(ctx, &repository.CurrencyOrderFilter{
			PageIdx:  pageIndex,
			PageSize: 200,
		})
		if err != nil {
			log.Error(err)
			break
		}
		if len(out) == 0 {
			break
		}
		pageIndex++

		for _, d := range out {
			var detail []byte
			if d.PaymentDetail != nil {
				detail, err = json.Marshal(d.PaymentDetail)
				if err != nil {
					log.Error(exutil.UUIDtoA(d.Id), ":", err)
					continue
				}
			}
			d.PayFundDetail = string(detail)
			repo.UpdateCurrencyOrder(ctx, d)
			fmt.Print(".")
		}
	}
	log.Info("finished")
}
