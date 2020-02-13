package main

import (
	"context"
	"net"
	"os"
	"sync"

	log "github.com/sirupsen/logrus"
	"gitlab.com/sdce/exlib/config"
	"gitlab.com/sdce/exlib/mongo"
	"gitlab.com/sdce/exlib/service"
	pb "gitlab.com/sdce/protogo"
	"gitlab.com/sdce/service/otc/pkg/api"
	"gitlab.com/sdce/service/otc/pkg/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

const (
	port          = "0.0.0.0:8030"
	consumerGroup = "service-otc"
)

type Service interface {
	Run(ctx context.Context)
}

type OtcService struct {
	rpc    *rpc.OtcServer
	health *health.Server
	db     *mongo.Database
}

func getConfig() (mongoConf *mongo.Config, svcConf service.Config, err error) {
	v, err := config.LoadConfig("service.otc")
	err = v.ReadInConfig()
	if err != nil {
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

func envOrDefaultString(envVar string, defaultValue string) string {
	value := os.Getenv(envVar)
	if value == "" {
		return defaultValue
	}

	return value
}

func (s *OtcService) Run(ctx context.Context) {
	lis, err := net.Listen("tcp", envOrDefaultString("otc_rpc:server:port", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	gs := grpc.NewServer()
	pb.RegisterOtcTradingServer(gs, s.rpc)
	grpc_health_v1.RegisterHealthServer(gs, s.health)
	s.health.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	// Register reflection service on gRPC server.
	reflection.Register(gs)

	go func() {
		select {
		case <-ctx.Done():
			gs.GracefulStop()
		}
	}()

	log.Infof("Listening at %v...\n", port)
	if err := gs.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func main() {
	log.SetReportCaller(true)
	log.SetFormatter(&log.JSONFormatter{})

	config.LoadConfig("service.otc")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgoConf, svcCfg, err := getConfig()
	db := mongo.Connect(ctx, *mgoConf)
	defer db.Close(ctx)
	log.Infof("service config: %v", svcCfg)

	api, err := api.New(&svcCfg)
	if err != nil {
		log.Fatal("failed to create api ")
	}
	otcServer := rpc.NewOtcTradingServer(api, db)
	otc := &OtcService{
		rpc:    otcServer,
		db:     db,
		health: health.NewServer(),
	}

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		otc.Run(ctx)
	}()
	wg.Wait()

}
