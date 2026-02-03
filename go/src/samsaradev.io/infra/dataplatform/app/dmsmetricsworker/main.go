package main

import (
	"go.uber.org/fx"

	"samsaradev.io/infra/config"
	"samsaradev.io/infra/monitoring/datadoghttp"
	"samsaradev.io/system"
)

func main() {
	system.NewFx(&config.ConfigParams{},
		fx.Provide(datadoghttp.NewDatadogHTTPClient),
		fx.Invoke(func(*Worker) {}),
	).Run()
}
