package main

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"go.uber.org/fx"

	"samsaradev.io/infra/config"
	_ "samsaradev.io/infra/dataplatform/databricksoauthfx"
	"samsaradev.io/infra/monitoring/datadoghttp"
	"samsaradev.io/infra/samsaraaws/awshelpers/awssessions"
	"samsaradev.io/system"
)

func main() {
	system.NewFx(&config.ConfigParams{},
		fx.Provide(datadoghttp.NewDatadogHTTPClient),
		fx.Provide(func() *session.Session { return awssessions.NewInstrumentedAWSSessionWithConfigs() }),
		fx.Invoke(func(*Worker) {}),
	).Run()
}
