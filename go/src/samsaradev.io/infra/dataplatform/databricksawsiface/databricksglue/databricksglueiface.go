package databricksglue

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/glue"
	"github.com/aws/aws-sdk-go/service/glue/glueiface"

	"samsaradev.io/infra/config"
	"samsaradev.io/infra/dataplatform/dataplatformhelpers"
	"samsaradev.io/infra/samsaraaws/awshelpers/awssessions"
)

func NewGlue(appConfig *config.AppConfig) glueiface.GlueAPI {
	sess := awssessions.NewInstrumentedAWSSession()
	region := aws.StringValue(sess.Config.Region)
	return glue.New(dataplatformhelpers.GetDatabricksAWSSession(region))
}
