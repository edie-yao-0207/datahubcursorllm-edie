package datahubprojects

import (
	"fmt"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformprojecthelpers"
)

var ECRRepositories []string = []string{
	"frontend-prod",
	"frontend-staging",
	"gms",
	"mae-consumer",
	"mce-consumer",
	"kafka-setup",
	"mysql-setup",
	"upgrade",
	"elasticsearch-setup",
	"acryl-datahub-actions",
	"ingestion-base",
}

func ecrResources() map[string][]tf.Resource {
	resources := []tf.Resource{}

	for _, r := range ECRRepositories {
		repo := awsresource.ECRRepository{
			Name: fmt.Sprintf("%s-%s", DatahubResourceBaseName, r),
			Tags: getDatahubTags(fmt.Sprintf("%s-ecr-repo", r)),
		}
		repo.BaseResource.MetaParameters.Lifecycle.PreventDestroy = true
		policy := dataplatformprojecthelpers.GenerateEcrLifecyclePolicy(repo)
		resources = append(resources, &repo, &policy)

	}
	return map[string][]tf.Resource{
		"ecr": resources,
	}
}
