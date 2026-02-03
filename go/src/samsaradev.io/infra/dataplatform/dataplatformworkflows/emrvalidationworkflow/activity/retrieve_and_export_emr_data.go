package activity

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/samsarahq/go/oops"
	"go.uber.org/fx"

	"samsaradev.io/helpers/appenv"
	"samsaradev.io/helpers/slog"
	"samsaradev.io/infra/workflows/workflowregistry"
	"samsaradev.io/libs/ni/pointer"
	"samsaradev.io/platform/entity/entityconstants"
	"samsaradev.io/platform/entity/entitydataaccessor"
	"samsaradev.io/platform/entity/entitymetadataaccessor"
	"samsaradev.io/platform/entity/entityproto"
	"samsaradev.io/platform/entity/types"
	"samsaradev.io/platform/entity/types/value"
)

func init() {
	workflowregistry.MustRegisterActivityConstructor(NewRetrieveAndExportEmrDataActivity)
}

type RetrieveAndExportEmrDataActivityArgs struct {
	EntityName          string
	SelectedOrgs        []OrgAssetSelection
	ValidationDate      time.Time
	S3Path              string
	S3Region            string
	ValidationTimestamp string
}

type EmrDataRecord struct {
	OrgId     int64       `json:"orgId"`
	AssetId   int64       `json:"assetId"`
	Data      value.Value `json:"data"` // Use value.Value for proper EMR data representation
	Timestamp time.Time   `json:"timestamp"`
}

type ExportedFile struct {
	S3Path      string    `json:"s3Path"`
	Size        int64     `json:"size"`
	RecordCount int       `json:"recordCount"`
	Timestamp   time.Time `json:"timestamp"`
}

type RetrieveAndExportEmrDataActivityResult struct {
	ExportedFiles []ExportedFile `json:"exportedFiles"`
	TotalRecords  int            `json:"totalRecords"`
}

type RetrieveAndExportEmrDataActivity struct {
	EntityDataAccessor     entitydataaccessor.Accessor
	EntityMetadataAccessor entitymetadataaccessor.Accessor
}

type RetrieveAndExportEmrDataActivityParams struct {
	fx.In
	EntityDataAccessor     entitydataaccessor.Accessor
	EntityMetadataAccessor entitymetadataaccessor.Accessor
}

func NewRetrieveAndExportEmrDataActivity(p RetrieveAndExportEmrDataActivityParams) *RetrieveAndExportEmrDataActivity {
	return &RetrieveAndExportEmrDataActivity{
		EntityDataAccessor:     p.EntityDataAccessor,
		EntityMetadataAccessor: p.EntityMetadataAccessor,
	}
}

func (a RetrieveAndExportEmrDataActivity) Name() string {
	return "RetrieveAndExportEmrDataActivity"
}

func (a RetrieveAndExportEmrDataActivity) Execute(ctx context.Context, args *RetrieveAndExportEmrDataActivityArgs) (RetrieveAndExportEmrDataActivityResult, error) {
	slog.Infow(ctx, "EMR Validation: Starting EMR data retrieval and export",
		"entityName", args.EntityName,
		"numOrgs", len(args.SelectedOrgs),
		"date", args.ValidationDate.Format("2006-01-02"),
		"s3Path", args.S3Path)

	// Step 1: Retrieve EMR data
	allData, err := a.retrieveEmrData(ctx, args)
	if err != nil {
		return RetrieveAndExportEmrDataActivityResult{}, oops.Wrapf(err, "failed to retrieve EMR data")
	}

	slog.Infow(ctx, "EMR Validation: Retrieved EMR data, starting export",
		"totalRecords", len(allData))

	// Step 2: Export data to S3
	exportedFiles, err := a.exportDataToS3(ctx, args, allData)
	if err != nil {
		return RetrieveAndExportEmrDataActivityResult{}, oops.Wrapf(err, "failed to export data to S3")
	}

	slog.Infow(ctx, "EMR Validation: Completed EMR data retrieval and export",
		"totalRecords", len(allData),
		"exportedFiles", len(exportedFiles))

	return RetrieveAndExportEmrDataActivityResult{
		ExportedFiles: exportedFiles,
		TotalRecords:  len(allData),
	}, nil
}

// retrieveEmrData handles the EMR data retrieval logic
func (a RetrieveAndExportEmrDataActivity) retrieveEmrData(ctx context.Context, args *RetrieveAndExportEmrDataActivityArgs) ([]EmrDataRecord, error) {
	var allData []EmrDataRecord

	// Process each org separately
	for i, orgSelection := range args.SelectedOrgs {
		slog.Infow(ctx, "EMR Validation: Processing org",
			"orgIndex", i+1,
			"totalOrgs", len(args.SelectedOrgs),
			"orgId", orgSelection.OrgId,
			"numAssets", len(orgSelection.AssetIds))

		emrData, err := a.getEmrDataForOrg(ctx, args.EntityName, orgSelection.OrgId, orgSelection.AssetIds, args.ValidationDate)
		if err != nil {
			// Log error but continue with other orgs for testing purposes for now.
			// TODO: Once we've tested the workflow's functionality in production environment,
			// we should return an error if we can't get the EMR data for an org.
			slog.Errorw(ctx, oops.Wrapf(err, "EMR Validation: Failed to get EMR data for org"),
				"orgId", orgSelection.OrgId,
				"entityName", args.EntityName)
			continue
		}

		// Convert EMR data to records
		records, err := a.convertEmrDataToRecords(emrData, orgSelection.OrgId, args.ValidationDate)
		if err != nil {
			// Log error but continue with other orgs for testing purposes for now.
			// TODO: Once we've tested the workflow's functionality in production environment,
			// we should return an error if we can't convert the EMR data for an org.
			slog.Errorw(ctx, oops.Wrapf(err, "EMR Validation: Failed to convert EMR data for org"),
				"orgId", orgSelection.OrgId,
				"entityName", args.EntityName)
			continue
		}

		slog.Infow(ctx, "EMR Validation: Retrieved records for org",
			"orgId", orgSelection.OrgId,
			"numRecords", len(records))

		allData = append(allData, records...)
	}

	return allData, nil
}

// exportDataToS3 handles the S3 export logic
func (a RetrieveAndExportEmrDataActivity) exportDataToS3(ctx context.Context, args *RetrieveAndExportEmrDataActivityArgs, allData []EmrDataRecord) ([]ExportedFile, error) {
	// Create AWS session using default credential chain
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(args.S3Region),
	})

	// TODO: Remove once we've tested the workflow's functionality in production environment.
	// Create AWS session with playground profile for write access
	// sess, err := session.NewSessionWithOptions(session.Options{
	// 	SharedConfigState: session.SharedConfigEnable,
	// 	Profile:           "sso-playground", // Use playground profile for aws-eng-playground-global account
	// 	Config: aws.Config{
	// 		Region: aws.String(args.S3Region),
	// 	},
	// })
	if err != nil {
		return nil, oops.Wrapf(err, "failed to create AWS session")
	}

	s3Client := s3.New(sess)

	// Parse S3 path to extract bucket and key prefix
	bucket, keyPrefix, err := a.parseS3Path(args.S3Path)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to parse S3 path: %s", args.S3Path)
	}

	timestamp := time.Now()

	// Group data by org
	orgData := make(map[int64][]EmrDataRecord)
	for _, record := range allData {
		orgData[record.OrgId] = append(orgData[record.OrgId], record)
	}

	var exportedFiles []ExportedFile

	// Create a file for each selected org (even if it has no data)
	for _, orgSelection := range args.SelectedOrgs {
		orgId := orgSelection.OrgId
		filename := fmt.Sprintf("%s_org_%d_%s.json", args.EntityName, orgId, timestamp.Format("20060102_150405"))
		s3Key := keyPrefix + filename

		// Get records for this org (empty slice if no data)
		records := orgData[orgId]
		if records == nil {
			records = []EmrDataRecord{} // Ensure we have an empty slice, not nil
		}

		// Convert records to JSON (will be empty array if no records)
		jsonData, err := json.MarshalIndent(records, "", "  ")
		if err != nil {
			return nil, oops.Wrapf(err, "failed to marshal data for org %d", orgId)
		}

		// Upload to S3
		_, err = s3Client.PutObjectWithContext(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(bucket),
			Key:         aws.String(s3Key),
			Body:        strings.NewReader(string(jsonData)),
			ContentType: aws.String("application/json"),
		})
		if err != nil {
			return nil, oops.Wrapf(err, "failed to upload data for org %d to S3", orgId)
		}

		exportedFiles = append(exportedFiles, ExportedFile{
			S3Path:      fmt.Sprintf("s3://%s/%s", bucket, s3Key),
			Size:        int64(len(jsonData)),
			RecordCount: len(records),
			Timestamp:   timestamp,
		})

		slog.Infow(ctx, "EMR Validation: Exported data for org",
			"orgId", orgId,
			"s3Path", fmt.Sprintf("s3://%s/%s", bucket, s3Key),
			"recordCount", len(records),
			"size", len(jsonData))
	}

	return exportedFiles, nil
}

// getEmrDataForOrg retrieves EMR data for a specific org
func (a RetrieveAndExportEmrDataActivity) getEmrDataForOrg(ctx context.Context, entityName string, orgId int64, assetIds []int64, date time.Time) (value.Value, error) {
	authzPrincipalId := &entityproto.AuthzPrincipalId{
		InternalServiceName: pointer.New(appenv.AppName()),
	}

	// Get entity metadata
	entityMap, err := a.EntityMetadataAccessor.GetFullEntityMap(ctx, orgId)
	if err != nil {
		return value.Value{}, oops.Wrapf(err, "failed to get entity map for org %d", orgId)
	}

	entity := entityMap.Get(entityName)
	if entity == nil {
		return value.Value{}, oops.Errorf("entity not found for name: %s", entityName)
	}

	// Build filters for the query
	var filtersBuilder *value.ObjectBuilder
	filtersField := entity.GetApiEndpoint(entityproto.ApiEndpoint_ListRecords).RequestType().Field(entityconstants.ApiRequestFiltersField)
	if filtersField != nil && filtersField.Type().AsObject() != nil {
		filtersBuilder = value.NewObjectBuilder(filtersField.Type().AsObject())

		// Add asset ID filters if provided
		if len(assetIds) > 0 {
			assetIdsInField := filtersField.Type().AsObject().Field("assetIdIn")
			if assetIdsInField != nil {
				assetIdsInType := assetIdsInField.Type()
				assetIdType := assetIdsInType.AsArray().ItemType()
				assetIdValues := make([]value.Value, len(assetIds))
				for i, id := range assetIds {
					assetIdValue, err := value.NewStringValue(assetIdType, fmt.Sprintf("%d", id))
					if err != nil {
						return value.Value{}, oops.Wrapf(err, "failed to create asset ID value")
					}
					assetIdValues[i] = assetIdValue
				}

				assetIdArray, err := value.NewArrayValue(assetIdsInType, assetIdValues...)
				if err != nil {
					return value.Value{}, oops.Wrapf(err, "failed to build asset ID array")
				}

				if err := filtersBuilder.SetFieldByName("assetIdIn", assetIdArray); err != nil {
					return value.Value{}, oops.Wrapf(err, "failed to set assetIdIn filter")
				}
			}
		}

		// Add date filter for the specific day
		dateFilterField := a.findDateFilterField(filtersField.Type().AsObject())
		if dateFilterField != "" {
			// Set start of day
			startOfDay := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, date.Location())
			endOfDay := startOfDay.Add(24 * time.Hour)

			startTimeMs := startOfDay.UnixMilli()
			endTimeMs := endOfDay.UnixMilli()

			// Try to set greater than or equal filter
			if startField := filtersField.Type().AsObject().Field(dateFilterField + "GreaterThanOrEqual"); startField != nil {
				startValue, err := value.NewIntegerValue(startField.Type(), startTimeMs)
				if err != nil {
					return value.Value{}, oops.Wrapf(err, "failed to create start time value")
				}
				if err := filtersBuilder.SetField(startField, startValue); err != nil {
					return value.Value{}, oops.Wrapf(err, "failed to set start time filter")
				}
			}

			// Try to set less than filter
			if endField := filtersField.Type().AsObject().Field(dateFilterField + "LessThan"); endField != nil {
				endValue, err := value.NewIntegerValue(endField.Type(), endTimeMs)
				if err != nil {
					return value.Value{}, oops.Wrapf(err, "failed to create end time value")
				}
				if err := filtersBuilder.SetField(endField, endValue); err != nil {
					return value.Value{}, oops.Wrapf(err, "failed to set end time filter")
				}
			}
		} else {
			slog.Warnw(ctx, "EMR Validation: No date filter field found for entity", "entityName", entityName)
			return value.Value{}, oops.Errorf("no date filter field found for entity %s", entityName)
		}
	}

	queryArgs := &entitydataaccessor.QueryArguments{}
	if filtersBuilder != nil {
		queryArgs.Filters = filtersBuilder
	}

	// Get data from EMR API - this will paginate automatically
	var allData []value.Value
	var pageToken *string
	var firstPageType *types.Type

	for {
		data, nextPageToken, err := a.EntityDataAccessor.GetUnjoinedEntitiesPaginated(ctx, orgId, authzPrincipalId, entity, queryArgs, pageToken)
		if err != nil {
			return value.Value{}, oops.Wrapf(err, "failed to get unjoined entities page for org %d", orgId)
		}

		// Convert data to array and append items
		dataArray, err := data.AsArray()
		if err != nil {
			return value.Value{}, oops.Wrapf(err, "failed to convert data to array")
		}

		// Store the array type from the first page for later use
		if firstPageType == nil {
			firstPageType = dataArray.Type()
		}

		allData = append(allData, dataArray.Items()...)

		if nextPageToken == nil {
			break
		}
		pageToken = nextPageToken
	}

	// Create result array using the type from the first page
	if firstPageType != nil {
		return value.NewArrayValue(firstPageType, allData...)
	}

	// If we never got any pages (shouldn't happen), return empty value
	return value.Value{}, nil
}

// findDateFilterField finds the appropriate date filter field for the entity
func (a RetrieveAndExportEmrDataActivity) findDateFilterField(filtersType *types.ObjectType) string {
	// Common date field patterns in EMR entities
	dateFields := []string{"tripStartTime", "startTime", "timestamp", "createdAt", "occurredAt"}

	for _, field := range dateFields {
		if filtersType.Field(field+"GreaterThanOrEqual") != nil {
			return field
		}
	}

	return ""
}

// convertEmrDataToRecords converts EMR value.Value data to structured records
func (a RetrieveAndExportEmrDataActivity) convertEmrDataToRecords(emrData value.Value, orgId int64, date time.Time) ([]EmrDataRecord, error) {
	// Handle empty/nil data gracefully
	if emrData.IsNil() {
		return []EmrDataRecord{}, nil
	}

	dataArray, err := emrData.AsArray()
	if err != nil {
		return nil, oops.Wrapf(err, "failed to convert EMR data to array")
	}

	var records []EmrDataRecord
	for _, item := range dataArray.Items() {
		// Extract asset ID from the item
		var assetId int64

		// Try to get the object value
		obj, err := item.AsObject()
		if err != nil {
			slog.Warnw(context.Background(), "EMR Validation: Failed to convert item to object",
				"orgId", orgId,
				"error", err)
			continue
		}

		// Try to get the assetId field value
		assetIdValue := obj.Field("assetId")
		if !assetIdValue.IsNil() {
			// Try as string first
			if str, err := assetIdValue.AsString(); err == nil {
				// Try parsing string asset ID
				if id, err := strconv.ParseInt(str, 10, 64); err == nil {
					assetId = id
				}
			} else {
				// Try as integer
				if num, err := assetIdValue.AsInt64(); err == nil {
					assetId = num
				}
			}
		}

		// Skip records where we couldn't determine the asset ID
		if assetId == 0 {
			slog.Warnw(context.Background(), "EMR Validation: Skipping record with no asset ID",
				"orgId", orgId,
				"data", item)
			continue
		}

		record := EmrDataRecord{
			OrgId:     orgId,
			AssetId:   assetId,
			Data:      item,
			Timestamp: date,
		}
		records = append(records, record)
	}

	return records, nil
}

// parseS3Path parses an S3 path into bucket and key components
func (a RetrieveAndExportEmrDataActivity) parseS3Path(s3Path string) (bucket, key string, err error) {
	if !strings.HasPrefix(s3Path, "s3://") {
		return "", "", oops.Errorf("invalid S3 path format: %s", s3Path)
	}

	path := strings.TrimPrefix(s3Path, "s3://")
	parts := strings.SplitN(path, "/", 2)

	bucket = parts[0]
	if bucket == "" {
		return "", "", oops.Errorf("invalid S3 path format: bucket name cannot be empty: %s", s3Path)
	}

	if len(parts) > 1 {
		key = parts[1]
	}

	return bucket, key, nil
}
