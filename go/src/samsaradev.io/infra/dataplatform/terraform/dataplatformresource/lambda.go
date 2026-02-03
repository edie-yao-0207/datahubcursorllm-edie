package dataplatformresource

import (
	"fmt"

	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
)

func LambdaArn(providerGroup string, region string, lambdaName string) string {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)

	// Should never happen since the providerGroup will always have a provider config.
	// It's safe to panic here because it's only panic-ing during terraform generation.
	// TODO: this panic should really be inside DatabricksProviderConfig
	if err != nil {
		panic(fmt.Sprintf("Provider Group %s is invalid. Error: %v\n", providerGroup, err))
	}

	accountId := config.DatabricksAWSAccountId
	return fmt.Sprintf("arn:aws:lambda:%s:%d:function:%s", region, accountId, lambdaName)
}
