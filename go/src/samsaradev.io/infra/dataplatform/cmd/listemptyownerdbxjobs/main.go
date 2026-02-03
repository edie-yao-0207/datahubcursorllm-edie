// When we remove a person from databricks, all jobs they own are now owned by an empty string
// This breaks permissioning for the job so the job will start failing
// This script helps us get ahead of the issue by identifying the jobs that no longer have an owner
package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/dataplatformhelpers"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/libs/ni/infraconsts"
)

func main() {
	ctx := context.Background()

	var region string
	flag.StringVar(&region, "region", "us-west-2", "region (us-west-2, eu-west-1, or ca-central-1), default: us-west-2")
	flag.Parse()

	client, err := dataplatformhelpers.GetDatabricksE2Client(region)
	if err != nil {
		log.Fatalln(err)
	}

	jobs, err := client.ListJobs(ctx, &databricks.ListJobsInput{})
	if err != nil {
		log.Fatalln(err)
	}

	for _, job := range jobs.Jobs {
		if job.CreatorUserName == "" {
			log.Println(job.JobId)
			log.Println(job.JobSettings.Name)
			urlBase := infraconsts.SamsaraDevDatabricksWorkspaceURLBase
			workspaceId := dataplatformconsts.DatabricksDevUSWorkspaceId
			if region == infraconsts.SamsaraAWSEURegion {
				urlBase = infraconsts.SamsaraEUDevDatabricksWorkspaceURLBase
				workspaceId = dataplatformconsts.DatabricksDevEUWorkspaceId
			} else if region == infraconsts.SamsaraAWSCARegion {
				urlBase = infraconsts.SamsaraCADevDatabricksWorkspaceURLBase
				workspaceId = dataplatformconsts.DatabricksDevCAWorkspaceId
			}

			log.Println(fmt.Sprintf("%s?o=%d#job/%d", urlBase, workspaceId, job.JobId))
			log.Println("------------------------------")
		}
	}

}
