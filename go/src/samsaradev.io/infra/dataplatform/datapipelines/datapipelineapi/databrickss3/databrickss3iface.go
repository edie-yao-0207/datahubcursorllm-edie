package databrickss3

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"

	"samsaradev.io/infra/config"
	"samsaradev.io/infra/fxregistry"
	"samsaradev.io/infra/samsaraaws/awshelpers/awssessions"
)

type S3API interface {
	s3iface.S3API
}

func init() {
	fxregistry.MustRegisterDefaultConstructor(NewS3)
}

func NewS3(appConfig *config.AppConfig) (S3API, error) {
	var sess *session.Session
	if appConfig.IsDevEnv() {
		sess = awssessions.NewInstrumentedAWSSessionWithOptions(session.Options{Profile: "databricks"})
	} else {
		creds := stscreds.NewCredentials(session.Must(session.NewSession()), appConfig.DatabricksAWSToolshedRole)
		sess = awssessions.NewInstrumentedAWSSessionWithConfigs(&aws.Config{Credentials: creds})
	}
	return s3.New(sess), nil
}
