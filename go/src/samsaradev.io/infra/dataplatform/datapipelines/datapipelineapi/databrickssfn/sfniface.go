package databrickssfn

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sfn"
	"github.com/aws/aws-sdk-go/service/sfn/sfniface"

	"samsaradev.io/infra/config"
	"samsaradev.io/infra/fxregistry"
	"samsaradev.io/infra/samsaraaws/awshelpers/awssessions"
)

type SFNAPI interface {
	sfniface.SFNAPI
}

func init() {
	fxregistry.MustRegisterDefaultConstructor(NewSFN)
}

func NewSFN(appConfig *config.AppConfig) (SFNAPI, error) {
	var sess *session.Session
	if appConfig.IsDevEnv() {
		sess = awssessions.NewInstrumentedAWSSessionWithOptions(session.Options{Profile: "databricks"})
	} else {
		creds := stscreds.NewCredentials(session.Must(session.NewSession()), appConfig.DatabricksAWSToolshedRole)
		sess = awssessions.NewInstrumentedAWSSessionWithConfigs(&aws.Config{Credentials: creds})
	}
	return sfn.New(sess), nil
}
