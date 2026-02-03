package emrvalidationworkflow

import (
	"errors"
	"fmt"
	"time"

	"go.uber.org/fx"

	"samsaradev.io/infra/dataplatform/dataplatformworkflows/emrvalidationworkflow/activity"
	"samsaradev.io/infra/dataplatform/emrreplication"
	"samsaradev.io/infra/workflows"
	"samsaradev.io/infra/workflows/client/workflowengine"
	"samsaradev.io/infra/workflows/workflowregistry"
	"samsaradev.io/infra/workflows/workflowsplatformconsts"
	"samsaradev.io/libs/ni/pointer"
	"samsaradev.io/team/teamnames"
)

func init() {
	workflowregistry.MustRegisterWorkflowConstructor(NewEmrValidationWorkflow)
}

type EmrValidationWorkflowParams struct {
	fx.In
	SelectOrgsAndAssetsActivity      *activity.SelectOrgsAndAssetsActivity
	RetrieveAndExportEmrDataActivity *activity.RetrieveAndExportEmrDataActivity
	WriteManifestActivity            *activity.WriteManifestActivity
	TriggerDatabricksJobActivity     *activity.TriggerDatabricksJobActivity
}

type EmrValidationWorkflowArgs struct {
	// Cell specifies which cell this workflow is running in (e.g., "us3")
	Cell string

	// ValidationDate specifies the date for which to validate EMR data
	ValidationDate *time.Time

	// EntityName specifies the EMR entity to export (e.g., "speedingintervalsbytrip", "trip", ...)
	// If empty, the workflow will iterate through all available entity types
	EntityName string

	// NumOrgsToValidate specifies the number of organizations to validate (default: 50)
	NumOrgsToValidate *int

	// NumAssetsToValidate specifies the number of assets per organization to validate (default: 20)
	NumAssetsToValidate *int

	// PinnedOrgs specifies org IDs that should always be included in validation runs
	PinnedOrgs []int64

	// S3Bucket specifies the S3 bucket to export to (e.g., "samsara-emr-replication-export-us3")
	S3Bucket string

	// S3Region specifies the AWS region for S3 operations
	S3Region string

	// RetrieveAndExportTimeoutMinutes specifies the timeout for the retrieve and export EMR data activity (default: 10)
	RetrieveAndExportTimeoutMinutes *int
}

type EmrValidationWorkflow struct {
	workflowengine.WorkflowImpl

	SelectOrgsAndAssetsActivity      *activity.SelectOrgsAndAssetsActivity
	RetrieveAndExportEmrDataActivity *activity.RetrieveAndExportEmrDataActivity
	WriteManifestActivity            *activity.WriteManifestActivity
	TriggerDatabricksJobActivity     *activity.TriggerDatabricksJobActivity
}

func NewEmrValidationWorkflow(p EmrValidationWorkflowParams) *EmrValidationWorkflow {
	return &EmrValidationWorkflow{
		SelectOrgsAndAssetsActivity:      p.SelectOrgsAndAssetsActivity,
		RetrieveAndExportEmrDataActivity: p.RetrieveAndExportEmrDataActivity,
		WriteManifestActivity:            p.WriteManifestActivity,
		TriggerDatabricksJobActivity:     p.TriggerDatabricksJobActivity,
	}
}

var EmrValidationWorkflowDefinition = workflows.CreateWorkflowDefinition[EmrValidationWorkflowArgs]("emr_validation", teamnames.DataPlatform, workflowsplatformconsts.Emrvalidationworkflowsworker)

func (EmrValidationWorkflow) Definition() workflows.WorkflowDefinition[EmrValidationWorkflowArgs] {
	return EmrValidationWorkflowDefinition
}

func (EmrValidationWorkflow) SupportedVersionFlags() []workflowengine.FlagKey {
	return []workflowengine.FlagKey{}
}

// getAssetTypeFromRegistry looks up the asset type for an entity in the EMR replication registry
func getAssetTypeFromRegistry(entityName string, getAllSpecs func() []emrreplication.EmrReplicationSpec) emrreplication.AssetType {
	for _, spec := range getAllSpecs() {
		if spec.Name == entityName {
			return spec.AssetType
		}
	}
	return emrreplication.DeviceAssetType // Default to device asset type
}

// processEntityValidation processes validation for a single entity type
func (w EmrValidationWorkflow) processEntityValidation(engine workflowengine.WorkflowEngine, args *EmrValidationWorkflowArgs, entityName string) error {
	logger := engine.GetLogger()
	currentWorkflowId := engine.CurrentWorkflowId()

	// Get asset type from EMR replication registry
	assetType := getAssetTypeFromRegistry(entityName, emrreplication.GetAllEmrReplicationSpecs)

	logger.Info("starting EMR validation export for entity",
		"workflowId", currentWorkflowId,
		"cell", args.Cell,
		"entityName", entityName,
		"assetType", assetType,
		"validationDate", args.ValidationDate.Format("2006-01-02"),
		"numOrgsValidated", *args.NumOrgsToValidate,
		"numAssetsValidated", *args.NumAssetsToValidate,
		"pinnedOrgs", args.PinnedOrgs,
		"s3Bucket", args.S3Bucket,
	)

	// Step 1: Select randomized orgs and assets
	logger.Info("selecting orgs and assets for validation", "entityName", entityName)
	selectResult, err := workflowengine.ExecuteActivity(engine,
		w.SelectOrgsAndAssetsActivity,
		&activity.SelectOrgsAndAssetsActivityArgs{
			Cell:               args.Cell,
			NumOrgsValidated:   *args.NumOrgsToValidate,
			NumAssetsValidated: *args.NumAssetsToValidate,
			PinnedOrgs:         args.PinnedOrgs,
			AssetType:          assetType,
		},
	)
	if err != nil {
		return workflowengine.WrapActivityErrorf(err, "failed to select orgs and assets for entity %s", entityName)
	}

	logger.Info("selected orgs and assets for validation",
		"entityName", entityName,
		"selectedOrgs", len(selectResult.SelectedOrgs),
		"totalAssets", func() int {
			total := 0
			for _, org := range selectResult.SelectedOrgs {
				total += len(org.AssetIds)
			}
			return total
		}())

	// Step 2: Retrieve EMR data and export to S3 in one operation
	validationTimestamp := engine.Now().Format("20060102_150405")
	s3DataPath := fmt.Sprintf("s3://%s/validation/entity=%s/date=%s/validation_timestamp=%s/data/",
		args.S3Bucket, entityName, args.ValidationDate.Format("2006-01-02"), validationTimestamp)

	logger.Info("retrieving EMR data and exporting to S3",
		"entityName", entityName,
		"selectedOrgs", len(selectResult.SelectedOrgs),
		"s3Path", s3DataPath)

	// Configure timeout for retrieve and export activity (default: 10 minutes)
	timeoutMinutes := 10
	if args.RetrieveAndExportTimeoutMinutes != nil {
		timeoutMinutes = *args.RetrieveAndExportTimeoutMinutes
	}

	retrieveAndExportResult, err := workflowengine.ExecuteActivity(engine,
		w.RetrieveAndExportEmrDataActivity,
		&activity.RetrieveAndExportEmrDataActivityArgs{
			EntityName:          entityName,
			SelectedOrgs:        selectResult.SelectedOrgs,
			ValidationDate:      *args.ValidationDate,
			S3Path:              s3DataPath,
			S3Region:            args.S3Region,
			ValidationTimestamp: validationTimestamp,
		},
		&workflowengine.ActivityOptions{
			StartToCloseTimeout: pointer.New(time.Minute * time.Duration(timeoutMinutes)),
		},
	)
	if err != nil {
		return workflowengine.WrapActivityErrorf(err, "failed to retrieve and export EMR data for entity %s", entityName)
	}

	logger.Info("completed EMR data retrieval and export",
		"entityName", entityName,
		"totalRecords", retrieveAndExportResult.TotalRecords,
		"exportedFiles", len(retrieveAndExportResult.ExportedFiles))

	// Step 3: Write manifest file
	manifestPath := fmt.Sprintf("s3://%s/validation/entity=%s/date=%s/validation_timestamp=%s/manifest.json",
		args.S3Bucket, entityName, args.ValidationDate.Format("2006-01-02"), validationTimestamp)
	logger.Info("writing manifest file", "entityName", entityName, "manifestPath", manifestPath)
	_, err = workflowengine.ExecuteActivity(engine,
		w.WriteManifestActivity,
		&activity.WriteManifestActivityArgs{
			ManifestPath:  manifestPath,
			S3Region:      args.S3Region,
			EntityName:    entityName,
			ExportedFiles: retrieveAndExportResult.ExportedFiles,
			SelectedOrgs:  selectResult.SelectedOrgs,
			ValidationRun: activity.ValidationRunMetadata{
				WorkflowId:     currentWorkflowId,
				Timestamp:      engine.Now(),
				Cell:           args.Cell,
				EntityName:     entityName,
				ValidationDate: *args.ValidationDate,
			},
		},
	)
	if err != nil {
		return workflowengine.WrapActivityErrorf(err, "failed to write manifest file for entity %s", entityName)
	}

	regionSuffixMap := map[string]string{
		"us-west-2":    "us",
		"eu-west-1":    "eu",
		"ca-central-1": "ca",
	}

	regionSuffix := regionSuffixMap[args.S3Region]

	// Step 4: Trigger databricks job to run the validation
	databricksJobName := fmt.Sprintf("emr-validation-%s-%s-%s-%s", entityName, args.Cell, regionSuffix, regionSuffix)

	logger.Info("triggering Databricks validation job", "jobName", databricksJobName)

	// Prepare parameters to pass to the Databricks job
	// Based on validate.go, the job expects these command-line parameters:
	// --region, --database, --entityname, --export-bucket, --serverless-env-vars, --validation-date, --decimal-tolerance
	databricksParams := []string{
		"--region", args.S3Region,
		"--database", fmt.Sprintf("emr_%s", args.Cell), // matches emrReplicationDatabase(cell) pattern
		"--entityname", entityName,
		"--export-bucket", fmt.Sprintf("emr-replication-export-%s", args.Cell), // matches emrReplicationExportBucket(cell) pattern
		"--validation-date", args.ValidationDate.Format("2006-01-02"),
		"--serverless-env-vars", `{"DATABRICKS_DEFAULT_SERVICE_CREDENTIAL_NAME":"emr-replication","AWS_REGION":"` + args.S3Region + `","AWS_DEFAULT_REGION":"` + args.S3Region + `"}`,
		"--decimal-tolerance",
	}

	databricksJobArgs := &activity.TriggerDatabricksJobActivityArgs{
		JobName:    &databricksJobName,
		Parameters: databricksParams,
	}

	databricksResult, err := workflowengine.ExecuteActivity(engine,
		w.TriggerDatabricksJobActivity,
		databricksJobArgs,
	)
	if err != nil {
		return workflowengine.WrapActivityErrorf(err, "failed to trigger Databricks validation job")
	}

	logger.Info("successfully triggered Databricks validation job",
		"runId", databricksResult.RunId,
		"jobName", databricksJobName)

	logger.Info("EMR validation completed successfully for entity",
		"entityName", entityName,
		"exportedFiles", len(retrieveAndExportResult.ExportedFiles),
		"totalRecords", retrieveAndExportResult.TotalRecords,
	)

	return nil
}

func (w EmrValidationWorkflow) Execute(engine workflowengine.WorkflowEngine, args *EmrValidationWorkflowArgs) error {
	// Check for nil parameters (this can happen during static analysis)
	if engine == nil {
		return errors.New("workflow engine cannot be nil")
	}

	defaultNumOrgs := 50
	defaultNumAssets := 20

	// Uncomment this to run the workflow with default values when testing locally
	// TODO: Remove this once we have a way to run the workflow with args passed in
	// via the UI, which is currently broken.
	// args = &EmrValidationWorkflowArgs{
	// 	Cell:                "us3",
	// 	EntityName:          "SpeedingIntervalsByTrip", // Optionally, do not specify this to process all entities
	// 	NumOrgsToValidate:   &defaultNumOrgs,
	// 	NumAssetsToValidate: &defaultNumAssets,
	// 	PinnedOrgs:          []int64{},
	// 	S3Region:            "us-west-2",
	// 	S3Bucket:            "timkim-emr-validation-export-test",
	// }

	if args == nil {
		return errors.New("workflow arguments cannot be nil")
	}

	currentWorkflowId := engine.CurrentWorkflowId()
	logger := engine.GetLogger()

	// Default values
	if args.NumOrgsToValidate == nil {
		args.NumOrgsToValidate = &defaultNumOrgs
	}
	if args.NumAssetsToValidate == nil {
		args.NumAssetsToValidate = &defaultNumAssets
	}
	if args.ValidationDate == nil {
		yesterday := engine.Now().AddDate(0, 0, -1)
		args.ValidationDate = &yesterday
	}
	if args.S3Bucket == "" {
		args.S3Bucket = fmt.Sprintf("samsara-emr-replication-export-%s", args.Cell)
	}

	logger.Info("starting EMR validation export workflow",
		"workflowId", currentWorkflowId,
		"cell", args.Cell,
		"entityName", args.EntityName,
		"validationDate", args.ValidationDate.Format("2006-01-02"),
		"numOrgsValidated", *args.NumOrgsToValidate,
		"numAssetsValidated", *args.NumAssetsToValidate,
		"pinnedOrgs", args.PinnedOrgs,
		"s3Bucket", args.S3Bucket,
	)

	// Determine which entities to process
	var entitiesToProcess []string
	if args.EntityName != "" {
		// Process single entity
		entitiesToProcess = []string{args.EntityName}
		logger.Info("processing single entity", "entityName", args.EntityName)
	} else {
		// Process all entities from registry
		allSpecs := emrreplication.GetAllEmrReplicationSpecs()
		for _, spec := range allSpecs {
			entitiesToProcess = append(entitiesToProcess, spec.Name)
		}
		logger.Info("processing all entities", "entityCount", len(entitiesToProcess), "entities", entitiesToProcess)
	}

	// Process each entity
	var successfulEntities []string
	var failedEntities []string

	for _, entityName := range entitiesToProcess {
		logger.Info("processing entity validation", "entityName", entityName)

		err := w.processEntityValidation(engine, args, entityName)
		if err != nil {
			logger.Error("failed to process entity validation", "entityName", entityName, "error", err)
			failedEntities = append(failedEntities, entityName)
			// Continue processing other entities instead of failing the entire workflow
			continue
		}

		successfulEntities = append(successfulEntities, entityName)
		logger.Info("successfully processed entity validation", "entityName", entityName)
	}

	logger.Info("EMR validation workflow completed",
		"workflowId", currentWorkflowId,
		"totalEntities", len(entitiesToProcess),
		"successfulEntities", len(successfulEntities),
		"failedEntities", len(failedEntities),
		"successful", successfulEntities,
		"failed", failedEntities,
	)

	// Return an error if any entities failed and we were processing a single entity
	// For multi-entity runs, we log failures but don't fail the entire workflow
	if len(failedEntities) > 0 && args.EntityName != "" {
		return fmt.Errorf("failed to process entity %s", args.EntityName)
	}

	return nil
}
