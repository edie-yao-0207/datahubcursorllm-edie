package main

import (
	"go.uber.org/fx"

	"samsaradev.io/infra/config"
	"samsaradev.io/infra/grpc/grpcmiddleware"
	"samsaradev.io/infra/retention/deleters/datalakedeleter"
	"samsaradev.io/system"
)

func main() {
	system.NewFx(&config.ConfigParams{},
		fx.Provide(
			func(s *datalakedeleter.DatalakeDeleter) grpcmiddleware.RunnableGRPCServer { return s },
		),
		grpcmiddleware.FxRunServer(),
	).Run()
}
