package main

import (
	"context"
	"sync"

	"gitlab.com/sdce/exlib/service"
	"gitlab.com/sdce/service/otc/pkg/expireworker"
	"gitlab.com/sdce/service/otc/pkg/otcapi"

	log "github.com/sirupsen/logrus"
	"gitlab.com/sdce/exlib/config"
	"gitlab.com/sdce/exlib/mongo"
)

const (
	serviceName = "service.otc"
)

func getConfigs() (mongoConf *mongo.Config, svcConf service.Config, err error) {
	v, err := config.LoadConfig(serviceName)
	if err != nil {
		log.Errorf("Failed to load configs: %v", err)
		return
	}

	mongoConf, err = mongo.GetConfig(v)
	if err != nil {
		return
	}
	svcConf, err = service.GetConfig(v)
	if err != nil {
		return
	}
	return
}

func main() {
	log.SetReportCaller(true)
	log.SetFormatter(&log.JSONFormatter{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Just in case

	mongoConf, svcConf, err := getConfigs()
	if err != nil {
		log.Fatalln("Cannot read config: ", err)
	}

	db := mongo.Connect(ctx, *mongoConf)
	defer db.Close(ctx)

	// ChangeStream consolidator is running in main app.

	otcapi, err := otcapi.New(&svcConf)
	if err != nil {
		log.Fatal("failed to create otc api ")
	}

	expireWorker := expireworker.NewExpireCheckService(otcapi, db)

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cancel()
		err := expireWorker.Run(ctx)
		log.Errorf("settlement service exited: %v", err)
	}()

	wg.Wait()
	log.Warning("Server has exited.")
}
