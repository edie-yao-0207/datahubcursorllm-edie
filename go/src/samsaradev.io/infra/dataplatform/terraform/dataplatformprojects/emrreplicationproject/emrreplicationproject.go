package emrreplicationproject

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/genericresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/emrreplication"
	"samsaradev.io/infra/dataplatform/emrreplication/emrhelpers"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
)

func EMRReplicationProjects(providerGroup string) ([]*project.Project, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "error creating databricks provider config for emr replication project")
	}

	projects := []*project.Project{}

	s3Project, err := s3Project(config)
	if err != nil {
		return nil, oops.Wrapf(err, "error creating s3 resources for emr replication project")
	}

	projects = append(projects, s3Project)

	unityCatalogInstanceProfileArn, err := dataplatformresource.InstanceProfileArn(config.DatabricksProviderGroup, "unity-catalog-cluster")
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	emrInstancePool, err := poolProject(config)
	if err != nil {
		return nil, oops.Wrapf(err, "error creating emr pool project")
	}

	remoteResources := remoteResources{
		infra: []tf.Resource{
			&genericresource.StringLocal{
				Name:  "unity_catalog_instance_profile",
				Value: fmt.Sprintf(`"%s"`, unityCatalogInstanceProfileArn),
			},
			&genericresource.StringLocal{
				Name:  "instance_profile",
				Value: fmt.Sprintf(`"%s"`, unityCatalogInstanceProfileArn),
			},
		},
		instancePool: emrInstancePool.remoteResources,
	}

	emrMergeSparkJobProject, err := emrMergeSparkJobProject(config, remoteResources)
	if err != nil {
		return nil, oops.Wrapf(err, "error creating emr merge spark job project")
	}

	projects = append(projects, emrMergeSparkJobProject, emrInstancePool.project)

	cdcProject, err := emrCdcProject(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "error creating cdc resources for emr replication project")
	}

	projects = append(projects, cdcProject)

	backfillProject, err := emrBackfillProject(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "error creating backfill resources for emr replication project")
	}
	projects = append(projects, backfillProject)

	viewsProject, err := EmrReplicationViewsProject(config)
	if err != nil {
		return nil, oops.Wrapf(err, "error creating speeding intervals view for emr replication project")
	}
	projects = append(projects, viewsProject)

	validationProject, err := emrValidationSparkJobProject(config, remoteResources)
	if err != nil {
		return nil, oops.Wrapf(err, "error creating validation resources for emr replication project")
	}
	projects = append(projects, validationProject)

	cells, err := emrhelpers.GetAllCellsPerRegion(config.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "error creating vacuum resources for emr replication project")
	}

	for _, entity := range emrreplication.GetAllEmrReplicationSpecs() {
		// The set of user specified cells that get their own jobs
		cellsWithDedicatedJobs := map[string]bool{}
		if entity.InternalOverrides != nil && entity.InternalOverrides.VacuumConfig != nil && len(entity.InternalOverrides.VacuumConfig.CellsWithDedicatedJobs) > 0 {
			for _, cell := range entity.InternalOverrides.VacuumConfig.CellsWithDedicatedJobs {
				if slices.Contains(cells, cell) {
					cellsWithDedicatedJobs[cell] = true
				} else {
					return nil, oops.Errorf("Problem with EMR replication entity '%s', InternalOverrides VacuumConfig field CellsWithDedicatedJobs value '%s', not a known cell. Valid options are: %s.", entity.Name, cell, strings.Join(cells, ", "))
				}
			}
		}

		// The set of cells that get batched, excluding cells that get their own job
		cellsToBatch := map[string]bool{}
		for _, cell := range cells {
			_, exists := cellsWithDedicatedJobs[cell]
			if !exists {
				cellsToBatch[cell] = true
			}
		}

		entityCells := []string{}
		for cell, _ := range cellsToBatch {
			entityCells = append(entityCells, cell)
		}
		sort.Strings(entityCells)

		numCellsToBatch := len(cellsToBatch)

		// Decide how many cells go into a minibatch
		// 4 was picked because that generally balances number of jobs, job
		// runtime, and job success
		cellsPerMinibatch := 4
		if entity.InternalOverrides != nil && entity.InternalOverrides.VacuumConfig != nil {
			if entity.InternalOverrides.VacuumConfig.Mode == emrreplication.VacuumModeAllCellsPerJob {
				// put all cells into a single minibatch
				cellsPerMinibatch = numCellsToBatch
			} else if entity.InternalOverrides.VacuumConfig.Mode == emrreplication.VacuumModeFixedCellsPerJob {
				// Positive integer value gets used
				cellsPerMinibatch = entity.InternalOverrides.VacuumConfig.CellsPerJob
			}
			// else use default
		}

		// Clip the number per minibatch between 1 and numCellsToBatch
		cellsPerMinibatch = max(min(cellsPerMinibatch, numCellsToBatch), 1)

		// Decide how many minibatches there'll be
		numMinibatches := numCellsToBatch / cellsPerMinibatch
		if numCellsToBatch%cellsPerMinibatch != 0 {
			numMinibatches += 1
		}

		// Create the resulting batched jobs
		numCellsBatched := 0
		for i := 0; i < numMinibatches; i++ {
			cellsMinibatch := entityCells[i*cellsPerMinibatch : min((i+1)*cellsPerMinibatch, numCellsToBatch)]
			numCellsBatched += len(cellsMinibatch)

			vacuumProject, err := emrVacuumProject(config, remoteResources, entity, cellsMinibatch, fmt.Sprintf("%d", i))
			if err != nil {
				return nil, oops.Wrapf(err, "error creating vacuum project")
			}
			projects = append(projects, vacuumProject)
		}

		if numCellsBatched != numCellsToBatch {
			return nil, oops.Errorf("Number of cells batched is %d , expected %d", numCellsBatched, numCellsToBatch)
		}

		// Create the individual jobs for cells that get their own job
		deterministicCellsWithDedicatedJobs := []string{}
		for cell, _ := range cellsWithDedicatedJobs {
			deterministicCellsWithDedicatedJobs = append(deterministicCellsWithDedicatedJobs, cell)
		}
		sort.Strings(deterministicCellsWithDedicatedJobs)

		for _, cell := range deterministicCellsWithDedicatedJobs {
			cellsMinibatch := []string{cell}
			vacuumProject, err := emrVacuumProject(config, remoteResources, entity, cellsMinibatch, cell)
			if err != nil {
				return nil, oops.Wrapf(err, "error creating vacuum project")
			}
			projects = append(projects, vacuumProject)
		}
	}

	for _, p := range projects {
		p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, map[string][]tf.Resource{
			"aws_provider": resource.ProjectAWSProvider(p),
			"tf_backend":   resource.ProjectTerraformBackend(p),
		})
	}

	return projects, nil
}
