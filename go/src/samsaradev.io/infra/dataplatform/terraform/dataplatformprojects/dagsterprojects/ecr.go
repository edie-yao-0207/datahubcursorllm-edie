package dagsterprojects

import (
	"fmt"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformprojecthelpers"
)

var ECRRepositories []string = []string{
	"datamodel-codeserver",
	"dataweb-codeserver",
	"ml-codeserver",
	"k8s",        // for building the dagster-k8s stock image
	"celery-k8s", // for building the dagster-celery-k8s stock image
}

func ecrResources() map[string][]tf.Resource {
	resources := []tf.Resource{}

	for _, r := range ECRRepositories {
		repo := awsresource.ECRRepository{
			Name: fmt.Sprintf("%s-%s", DagsterResourceBaseName, r),
			Tags: getDagsterTags(fmt.Sprintf("%s-ecr-repo", r)),
		}
		repo.BaseResource.MetaParameters.Lifecycle.PreventDestroy = true
		policy := dataplatformprojecthelpers.GenerateEcrLifecyclePolicy(repo)
		resources = append(resources, &repo, &policy)

	}
	return map[string][]tf.Resource{
		"ecr": resources,
	}
}
