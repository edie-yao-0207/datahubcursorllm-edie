package sparkk8s

import (
	"fmt"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
	"samsaradev.io/team"
)

const SparkK8sResourceBaseName = "spark-k8s"
const SparkK8slusterOIDCProvider = "oidc.eks.us-west-2.amazonaws.com/id/B47EB1131A423BA0E9845C566648D4D3"

// https://docs.aws.amazon.com/eks/latest/userguide/getting-started-console.html
func SparkK8sProject(databricksProviderGroup string) (*project.Project, error) {

	config, err := dataplatformconfig.DatabricksProviderConfig(databricksProviderGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to retrieve provider config")
	}

	p := &project.Project{
		RootTeam:        dataplatformterraformconsts.DataPlatformSparkK8sTerraformProjectPipeline,
		Provider:        config.DatabricksProviderGroup,
		GenerateOutputs: true,
		Class:           SparkK8sResourceBaseName,
		ResourceGroups:  project.MergeResourceGroups(),
	}

	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups,
		map[string][]tf.Resource{
			"aws_provider": resource.ProjectAWSProvider(p),
			"tf_backend":   resource.ProjectTerraformBackend(p),
		})

	return p, nil
}

// getEksTags generates a slice of tags for a Spark EKS resource based on the service name
func getEksTags(clusterName string) []*awsresource.Tag {
	return []*awsresource.Tag{
		{
			Key:   "samsara:team",
			Value: strings.ToLower(team.DataPlatform.TeamName),
		},
		{
			Key:   "samsara:product-group",
			Value: strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
		{
			Key:   "samsara:service",
			Value: fmt.Sprintf("spark-eks-%s", clusterName),
		},
	}
}
