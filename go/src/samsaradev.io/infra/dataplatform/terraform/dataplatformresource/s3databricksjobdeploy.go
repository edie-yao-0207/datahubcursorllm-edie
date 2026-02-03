package dataplatformresource

import (
	"fmt"
	"time"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/ni/sparkversion"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/libs/ni/policy"
	"samsaradev.io/service/pipelines"
	"samsaradev.io/team"
)

type DatabricksJobType string

const (
	DatabricksJobTypeS3BigStats DatabricksJobType = "s3bigstats"
	DatabricksJobTypeTest       DatabricksJobType = "test"
)

type DatabricksJob struct {
	DatabricksJobSpec JobSpec
	Pipeline          *pipelines.DatabricksPipelineGroup
	RolloutStage      pipelines.DatabricksJobRolloutState
	ScriptPath        string
	SchemaPath        string
	Type              DatabricksJobType
}

func adminPrincipal(region string, role string) map[string]string {
	accountID := infraconsts.GetAccountIdForRegion(region)
	return map[string]string{"AWS": fmt.Sprintf("arn:aws:iam::%d:role/%s", accountID, role)}
}

func S3DatabricksJobDeployProjects(providerGroup string) ([]*project.Project, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	p := &project.Project{
		RootTeam:        team.DataPlatform.Name(),
		Provider:        config.DatabricksProviderGroup,
		Class:           "s3databricksjobdeploy",
		ResourceGroups:  make(map[string][]tf.Resource),
		GenerateOutputs: true,
	}

	s3DatabricksJobResourcesBucket := Bucket{
		Name:                             "databricks-job-resources",
		Region:                           config.Region,
		NonCurrentExpirationDaysOverride: 2,
		Metrics:                          true,
		RnDCostAllocation:                0,
		SkipPublicAccessBlock:            config.Region == infraconsts.SamsaraAWSCARegion, // SCP forbids setting the public access block in CA region
	}

	s3DatabricksJobResourcesBucketPolicy := &awsresource.S3BucketPolicy{
		Bucket: s3DatabricksJobResourcesBucket.Bucket().ResourceId(),
		Policy: policy.AWSPolicy{
			Version: policy.AWSPolicyVersion,
			Statement: []policy.AWSPolicyStatement{
				{
					Principal: adminPrincipal(config.Region, "buildkite-ci-databricks-deploy"),
					Effect:    "Allow",
					Action:    []string{"s3:Get*", "s3:List*"},
					Resource: []string{
						s3DatabricksJobResourcesBucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", s3DatabricksJobResourcesBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
				},
				{
					Principal: adminPrincipal(config.Region, "buildkite-ci-databricks-deploy"),
					Effect:    "Allow",
					Action:    []string{"s3:PutObjectAcl", "s3:PutObject"},
					Resource: []string{
						s3DatabricksJobResourcesBucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", s3DatabricksJobResourcesBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
					Condition: &policy.AWSPolicyCondition{
						StringEquals: map[string]string{
							"s3:x-amz-acl": "bucket-owner-full-control",
						},
					},
				},
				{
					Principal: map[string]string{
						"AWS": fmt.Sprintf("arn:aws:iam::%d:root", infraconsts.GetAccountIdForRegion(config.Region)),
					},
					Effect: "Allow",
					Action: []string{"s3:PutObjectAcl", "s3:PutObject"},
					Resource: []string{
						s3DatabricksJobResourcesBucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", s3DatabricksJobResourcesBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
					Condition: &policy.AWSPolicyCondition{
						StringEquals: map[string]string{
							"s3:x-amz-acl": "bucket-owner-full-control",
						},
					},
				},
				{
					Principal: map[string]string{
						"AWS": fmt.Sprintf("arn:aws:iam::%d:root", infraconsts.GetAccountIdForRegion(config.Region)),
					},
					Effect: "Allow",
					Action: []string{"s3:Get*", "s3:List*"},
					Resource: []string{
						s3DatabricksJobResourcesBucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", s3DatabricksJobResourcesBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
				},
				{
					Principal: map[string]string{
						"AWS": fmt.Sprintf("arn:aws:iam::%d:root", infraconsts.GetDatabricksAccountIdForRegion(config.Region)),
					},
					Effect: "Allow",
					Action: []string{"s3:Get*", "s3:List*"},
					Resource: []string{
						s3DatabricksJobResourcesBucket.Bucket().ResourceId().ReferenceAttr("arn"),
						fmt.Sprintf("%s/*", s3DatabricksJobResourcesBucket.Bucket().ResourceId().ReferenceAttr("arn")),
					},
				},
			},
		},
	}

	var s3DatabricksJobResources []tf.Resource
	s3DatabricksJobResources = append(s3DatabricksJobResourcesBucket.Resources(), s3DatabricksJobResourcesBucketPolicy)

	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, map[string][]tf.Resource{
		"aws_provider":                resource.ProjectAWSProvider(p),
		"tf_backend":                  resource.ProjectTerraformBackend(p),
		"databricks_provider":         DatabricksOauthProvider(config.Hostname),
		"s3_databricks_job_resources": s3DatabricksJobResources,
	})
	return []*project.Project{p}, nil
}

func testJobResources(config dataplatformconfig.DatabricksConfig) (map[string][]tf.Resource, error) {
	jobs, err := DatabricksTestJobSpecs(config)
	if err != nil {
		return nil, oops.Wrapf(err, "error fetcching ")
	}

	resourceGroups := map[string][]tf.Resource{
		// 	"test_script": []tf.Resource{script},
	}

	for _, job := range jobs {
		jobSpec := job.DatabricksJobSpec
		jobResource, err := jobSpec.TfResource()
		if err != nil {
			return nil, oops.Wrapf(err, "building job resource")
		}
		jobResource.Lifecycle.IgnoreChanges = []string{tf.IgnoreAllChanges}
		permissionsResource, err := jobSpec.PermissionsResource()
		if err != nil {
			return nil, oops.Wrapf(err, "")
		}

		resourceGroups[jobSpec.Name] = []tf.Resource{jobResource, permissionsResource}
	}

	return resourceGroups, nil
}

func DatabricksTestJobSpecs(config dataplatformconfig.DatabricksConfig) ([]*DatabricksJob, error) {
	datapipelinesArn, err := InstanceProfileArn(config.DatabricksProviderGroup, "data-pipelines-cluster")
	if err != nil {
		return nil, oops.Wrapf(err, "Getting data-pipelines-cluster arn failed.")
	}

	names := []string{"beta", "stable"}
	jobs := make([]*DatabricksJob, len(names))
	for i, name := range names {
		parameters := []string{"--name", name}
		jobName := fmt.Sprintf("deploy-machinery-%s-resource", name)

		jobSpec := JobSpec{
			Name:               jobName,
			Parameters:         parameters,
			Region:             config.Region,
			Owner:              team.DataPlatform,
			SparkVersion:       sparkversion.DataPipelinesDeployMachineryDbrVersion,
			MinWorkers:         1,
			MaxWorkers:         1,
			Profile:            datapipelinesArn,
			TimeoutSeconds:     int((1 * time.Hour).Seconds()),
			EmailNotifications: []string{team.DataPlatform.SlackAlertsChannelEmail.Email},
			JobOwnerUser:       config.MachineUsers.CI.Email,
			Cron:               "", // these jobs are failing / we don't need them
			RnDCostAllocation:  1,
			JobType:            "deploy_machinery_test_job",
			Format:             databricks.MultiTaskKey,
			JobTags: map[string]string{
				"format": databricks.MultiTaskKey,
			},
		}

		job := &DatabricksJob{
			DatabricksJobSpec: jobSpec,
			ScriptPath:        "python3/samsaradev/infra/dataplatform/deploy_machinery_test_resource.py",
			Type:              DatabricksJobTypeTest,
		}
		if name == "beta" {
			job.RolloutStage = pipelines.DatabricksJobBetaRolloutState
		} else {
			job.RolloutStage = pipelines.DatabricksJobProdRolloutState
		}

		jobs[i] = job

	}
	return jobs, nil

}
