package databricksdeploymachineryregistry

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/libs/ni/infraconsts"
)

type s3BigStatsDeployMachineryPipeline struct {
	Type string
}

func NewS3BigStatsDeployMachineryPipeline() *s3BigStatsDeployMachineryPipeline {
	return &s3BigStatsDeployMachineryPipeline{
		Type: "s3bigstats",
	}
}

func (s *s3BigStatsDeployMachineryPipeline) TypeName() string {
	return s.Type
}

func (s *s3BigStatsDeployMachineryPipeline) AddJobsToDeploMachineryRegistry(config dataplatformconfig.DatabricksConfig) ([]*dataplatformresource.DatabricksJob, error) {
	poolResources, err := dataplatformprojects.S3BigStatsInstancePoolResources(config.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "error creating instance pool resources")
	}
	var allPoolResources []tf.Resource
	allPoolResources = append(allPoolResources, poolResources.NonproductionFleetDriver...)
	allPoolResources = append(allPoolResources, poolResources.NonproductionFleetWorker...)
	allPoolResources = append(allPoolResources, poolResources.ProductionFleetWorker...)
	allPoolResources = append(allPoolResources, poolResources.ProductionFleetDriver...)
	allPoolResources = append(allPoolResources, poolResources.ProductionFleetOndemand...)
	s3bigstatsJobs, _, err := dataplatformprojects.S3BigStatsDatabricksJobSpecs(config, poolResources)
	if err != nil {
		return nil, oops.Wrapf(err, "error getting s3bigstats databricks jobs in the deploy machinery registry")
	}
	return s3bigstatsJobs, err
}

func (s *s3BigStatsDeployMachineryPipeline) FetchDynamicFieldsForJobs(ctx context.Context, jobSettings databricks.JobSettings, databricksClient databricks.API, region string) (databricks.JobSettings, error) {
	databricksRegion := ""
	awsRegion := ""
	switch region {
	case "us":
		databricksRegion = "databricks-dev-us"
		awsRegion = infraconsts.SamsaraAWSDefaultRegion
	case "eu":
		databricksRegion = "databricks-dev-eu"
		awsRegion = infraconsts.SamsaraAWSEURegion
	case "ca":
		databricksRegion = "databricks-dev-ca"
		awsRegion = infraconsts.SamsaraAWSCARegion
	default:
		return databricks.JobSettings{}, oops.Errorf("%s", fmt.Sprintf("Unrecognized region:%s", region))
	}

	pools, err := databricksClient.ListInstancePools(ctx)
	if err != nil {
		return databricks.JobSettings{}, oops.Wrapf(err, "error getting instance pools from databricks")
	}

	poolResources, err := dataplatformprojects.S3BigStatsInstancePoolResources(awsRegion)
	if err != nil {
		return databricks.JobSettings{}, oops.Wrapf(err, "error getting data platform pool resources")
	}
	var allPoolResources []tf.Resource
	allPoolResources = append(allPoolResources, poolResources.NonproductionFleetDriver...)
	allPoolResources = append(allPoolResources, poolResources.NonproductionFleetWorker...)
	allPoolResources = append(allPoolResources, poolResources.ProductionFleetWorker...)
	allPoolResources = append(allPoolResources, poolResources.ProductionFleetDriver...)
	allPoolResources = append(allPoolResources, poolResources.ProductionFleetOndemand...)

	// Parse the tags to find the is-production-job tag to know
	// if the s3bigstats job is a production or non-production job
	isProduction := false
	for tag, value := range jobSettings.NewCluster.CustomTags {
		if strings.Contains(tag, "is-production-job") {
			isProduction, err = strconv.ParseBool(value)
			if err != nil {
				return databricks.JobSettings{}, oops.Wrapf(err, "error parsing is production job boolean")
			}
		}
	}

	// Use the production pool for production jobs, and hybrid pools for nonproduction jobs.
	worker := poolResources.NonproductionFleetWorker[0].ResourceId()
	driver := poolResources.NonproductionFleetDriver[0].ResourceId()
	if isProduction {
		worker = poolResources.ProductionFleetWorker[0].ResourceId()
		driver = poolResources.ProductionFleetDriver[0].ResourceId()
	}

	s3BigStatsImportARN, err := dataplatformresource.InstanceProfileArn(databricksRegion, "s3bigstats-import-cluster")
	if err != nil {
		return databricks.JobSettings{}, oops.Wrapf(err, "error getting s3 bigstats import arn")
	}

	var workerPoolId string
	var workerPoolName string
	var driverPoolId string
	var driverPoolName string
	for _, pool := range pools.InstancePools {
		if worker.Name == pool.InstancePoolSpec.InstancePoolName {
			workerPoolId = pool.InstancePoolId
			workerPoolName = pool.InstancePoolSpec.InstancePoolName
		}

		if driver.Name == pool.InstancePoolSpec.InstancePoolName {
			driverPoolId = pool.InstancePoolId
			driverPoolName = pool.InstancePoolSpec.InstancePoolName
		}
	}

	tags := jobSettings.NewCluster.CustomTags
	if tags == nil {
		tags = make(map[string]string)
	}
	tags["samsara:pooled-job:driver-pool-id"] = driverPoolId
	tags["samsara:pooled-job:driver-pool-name"] = driverPoolName
	tags["samsara:pooled-job:pool-id"] = workerPoolId
	tags["samsara:pooled-job:pool-name"] = workerPoolName

	jobSettings.NewCluster.CustomTags = tags
	jobSettings.NewCluster.InstancePoolId = workerPoolId
	jobSettings.NewCluster.DriverInstancePoolId = driverPoolId
	jobSettings.NewCluster.AwsAttributes.InstanceProfileArn = s3BigStatsImportARN

	return jobSettings, nil
}
