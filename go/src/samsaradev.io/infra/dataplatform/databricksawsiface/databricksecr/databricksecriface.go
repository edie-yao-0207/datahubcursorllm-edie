package databricksecr

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ecr"
	"github.com/aws/aws-sdk-go/service/ecr/ecriface"

	"samsaradev.io/infra/config"
	"samsaradev.io/infra/dataplatform/dataplatformhelpers"
	"samsaradev.io/infra/samsaraaws/awshelpers/awssessions"
)

func NewECR(appConfig *config.AppConfig) ecriface.ECRAPI {
	sess := awssessions.NewInstrumentedAWSSession()
	region := aws.StringValue(sess.Config.Region)
	return ecr.New(dataplatformhelpers.GetDatabricksAWSSession(region))
}
