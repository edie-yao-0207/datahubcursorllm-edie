package databricksdynamodb

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"

	"samsaradev.io/infra/config"
	"samsaradev.io/infra/fxregistry"
	"samsaradev.io/infra/samsaraaws/awshelpers/awssessions"
)

type DynamoDBAPI interface {
	dynamodbiface.DynamoDBAPI
}

func init() {
	fxregistry.MustRegisterDefaultConstructor(NewDynamoDB)
}

func NewDynamoDB(appConfig *config.AppConfig) (DynamoDBAPI, error) {
	var sess *session.Session
	if appConfig.IsDevEnv() {
		sess = awssessions.NewInstrumentedAWSSessionWithOptions(session.Options{Profile: "databricks"})
	} else {
		creds := stscreds.NewCredentials(session.Must(session.NewSession()), appConfig.DatabricksAWSToolshedRole)
		sess = awssessions.NewInstrumentedAWSSessionWithConfigs(&aws.Config{Credentials: creds})
	}
	return dynamodb.New(sess), nil
}
