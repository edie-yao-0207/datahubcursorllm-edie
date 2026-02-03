package activity

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/benbjohnson/clock"
	"github.com/samsarahq/go/oops"
	"go.uber.org/fx"

	"samsaradev.io/helpers/appenv"
	"samsaradev.io/helpers/monitoring"
	"samsaradev.io/helpers/samtime"
	"samsaradev.io/helpers/slog"
	"samsaradev.io/infra/dataplatform/dataplatformworkflows/emrbackfillworkflow/helpers"
	"samsaradev.io/infra/dataplatform/emrworker"
	"samsaradev.io/infra/samsaraaws"
	"samsaradev.io/infra/samsaraaws/s3iface"
	"samsaradev.io/infra/workflows/client/workflowengine"
	"samsaradev.io/infra/workflows/workflowregistry"
	"samsaradev.io/libs/ni/pointer"
	"samsaradev.io/platform/entity/entityconstants"
	"samsaradev.io/platform/entity/entitydataaccessor"
	"samsaradev.io/platform/entity/entitymetadataaccessor"
	"samsaradev.io/platform/entity/entityproto"
	"samsaradev.io/platform/entity/types/value"
)

const (
	maxRecordSize = 1024 * 1024     // 1MB in bytes
	maxBatchSize  = 5 * 1024 * 1024 // 5MB in bytes (Kinesis PutRecords limit)
	maxRetries    = 3               // Maximum number of retry attempts
	backoffBase   = 100             // Base backoff in milliseconds
)

const (
	streamEmrDataToKinesis_GetEmrData_Duration          = "emrbackfillworkflow.stream_emr_data_to_kinesis.get_emr_data.duration"
	streamEmrDataToKinesis_WriteBatchToKinesis_Duration = "emrbackfillworkflow.stream_emr_data_to_kinesis.write_batch_to_kinesis.duration"
	streamEmrDataToKinesis_SendBatchToKinesis_Duration  = "emrbackfillworkflow.stream_emr_data_to_kinesis.send_batch_to_kinesis.duration"

	streamEmrDataToKinesis_GetEmrDataError          = "emrbackfillworkflow.stream_emr_data_to_kinesis.get_emr_data_error"
	streamEmrDataToKinesis_WriteBatchToKinesisError = "emrbackfillworkflow.stream_emr_data_to_kinesis.write_batch_to_kinesis_error"
	streamEmrDataToKinesis_SendBatchToKinesisError  = "emrbackfillworkflow.stream_emr_data_to_kinesis.send_batch_to_kinesis_error"

	streamEmrDataToKinesis_GetEmrData_GetEntityMapError                   = "emrbackfillworkflow.stream_emr_data_to_kinesis.get_emr_data.get_entity_map_error"
	streamEmrDataToKinesis_GetEmrData_GetEntityFromMapError               = "emrbackfillworkflow.stream_emr_data_to_kinesis.get_emr_data.get_entity_from_map_error"
	streamEmrDataToKinesis_GetEmrData_GetUnjoinedEntitiesError            = "emrbackfillworkflow.stream_emr_data_to_kinesis.get_emr_data.get_unjoined_entities_error"
	streamEmrDataToKinesis_SendBatchToKinesis_RecordExceedsSizeLimitError = "emrbackfillworkflow.stream_emr_data_to_kinesis.send_batch_to_kinesis.record_exceeds_size_limit_error"
	streamEmrDataToKinesis_SendBatchToKinesis_PutRecordsError             = "emrbackfillworkflow.stream_emr_data_to_kinesis.send_batch_to_kinesis.put_records_error"
	streamEmrDataToKinesis_SendBatchToKinesis_NonThrottlingError          = "emrbackfillworkflow.stream_emr_data_to_kinesis.send_batch_to_kinesis.non_throttling_error"
	streamEmrDataToKinesis_SendBatchToKinesis_FailedRecordsAfterRetries   = "emrbackfillworkflow.stream_emr_data_to_kinesis.send_batch_to_kinesis.failed_records_after_retries"
	streamEmrDataToKinesis_SplitIntoBatches_RecordExceedsBatchSizeLimit   = "emrbackfillworkflow.stream_emr_data_to_kinesis.split_into_batches.record_exceeds_batch_size_limit"

	streamEmrDataToKinesis_S3_Offload_Error   = "emrbackfillworkflow.stream_emr_data_to_kinesis.s3_offload.error"
	streamEmrDataToKinesis_S3_Offload_Success = "emrbackfillworkflow.stream_emr_data_to_kinesis.s3_offload.success"
)

type kinesisRecord struct {
	OrgId             string                    `json:"orgId"`
	BackfillRequestId string                    `json:"backfillRequestId"`
	EmrResponse       *value.Value              `json:"emrResponse,omitempty"` // Inline data (for small records)
	S3Path            string                    `json:"s3Path,omitempty"`      // S3 path (for large records)
	EntityName        string                    `json:"entityName"`
	ChangeOperation   emrworker.ChangeOperation `json:"changeOperation"`
	ReceivedAt        time.Time                 `json:"receivedAt"`
}

func init() {
	workflowregistry.MustRegisterActivityConstructor(NewStreamEmrDataToKinesisActivity)
}

type StreamEmrDataToKinesisActivityArgs struct {
	OrgId             int64
	EntityName        string
	PageToken         *string
	PageSize          int64
	AssetIds          []int64
	StartMs           *int64
	EndMs             *int64
	BackfillRequestId string
	Filters           helpers.FilterFieldToValueMap
	FilterOptions     []helpers.FilterOptions
	BatchIndex        int
	FilterId          string
}

type StreamEmrDataToKinesisActivity struct {
	EntityDataAccessor     entitydataaccessor.Accessor
	EntityMetadataAccessor entitymetadataaccessor.Accessor
	KinesisClient          kinesisiface.KinesisAPI
	S3Client               s3iface.S3API
	Clock                  clock.Clock
}

type StreamEmrDataToKinesisActivityParams struct {
	fx.In
	EntityDataAccessor     entitydataaccessor.Accessor
	EntityMetadataAccessor entitymetadataaccessor.Accessor
	KinesisClient          kinesisiface.KinesisAPI
	S3Client               s3iface.S3API
	Clock                  clock.Clock
}

func NewStreamEmrDataToKinesisActivity(p StreamEmrDataToKinesisActivityParams) *StreamEmrDataToKinesisActivity {
	return &StreamEmrDataToKinesisActivity{
		EntityDataAccessor:     p.EntityDataAccessor,
		EntityMetadataAccessor: p.EntityMetadataAccessor,
		KinesisClient:          p.KinesisClient,
		S3Client:               p.S3Client,
		Clock:                  p.Clock,
	}
}

func (a StreamEmrDataToKinesisActivity) Name() string {
	return "StreamEmrDataToKinesisActivity"
}

func (a StreamEmrDataToKinesisActivity) Execute(ctx context.Context, args *StreamEmrDataToKinesisActivityArgs) (workflowengine.NilActivityResult, error) {
	totalRecords := int64(0)
	pageToken := args.PageToken

	for {
		additionalTags := slog.Tag{"pageToken": pageToken, "batchIndex": args.BatchIndex}
		// skiplint: +slogsensitivedata
		slog.Infow(ctx, "EMR Backfill: Getting EMR data", helpers.GetSlogInfoTags(args.OrgId, args.EntityName, args.BackfillRequestId, additionalTags)...)
		// Update args with current page token
		args.PageToken = pageToken
		getEmrDataResultResp, err := a.getEmrData(ctx, args)
		if err != nil {
			monitoring.AggregatedDatadog.Incr(streamEmrDataToKinesis_GetEmrDataError)
			return workflowengine.NilActivityResult{}, oops.Wrapf(err, "failed to get emr data")
		}
		// Get the EMR data as an array.
		emrDataArray, emrDataErr := getEmrDataResultResp.EmrData.AsArray()
		if emrDataErr != nil {
			wrappedErr := oops.Wrapf(emrDataErr, "EMR Backfill: failed to convert EMR data to array")
			slog.Errorw(ctx, wrappedErr, helpers.GetSlogErrorTags(args.OrgId, args.EntityName, args.BackfillRequestId, additionalTags))
			return workflowengine.NilActivityResult{}, wrappedErr
		}

		// Split the data into batches.
		items := emrDataArray.Items()
		slog.Infow(ctx, "EMR Backfill: Got EMR data", helpers.GetSlogInfoTags(args.OrgId, args.EntityName, args.BackfillRequestId, slog.Tag{"numItems": len(items), "pageToken": pageToken, "batchIndex": args.BatchIndex})...)
		kinesisBatchSize := int(args.PageSize)
		for kinesisBatchStart := 0; kinesisBatchStart < len(items); kinesisBatchStart += kinesisBatchSize {
			additionalKinesisTags := slog.Tag{"kinesisBatchStart": kinesisBatchStart, "kinesisBatchSize": kinesisBatchSize, "pageToken": pageToken, "batchIndex": args.BatchIndex}
			end := kinesisBatchStart + kinesisBatchSize
			if end > len(items) {
				end = len(items)
			}
			slog.Infow(ctx, fmt.Sprintf("EMR Backfill: Starting to write %d/%d items to kinesis", end, len(items)), helpers.GetSlogInfoTags(args.OrgId, args.EntityName, args.BackfillRequestId, slog.Tag{"numItems": len(items), "pageToken": pageToken, "batchIndex": args.BatchIndex})...)

			// Create a batch of items.
			batchItems := items[kinesisBatchStart:end]
			batchValue, err := value.NewArrayValue(emrDataArray.Type(), batchItems...)
			if err != nil {
				wrappedErr := oops.Wrapf(err, "EMR Backfill: failed to create batch array value")
				slog.Errorw(ctx, wrappedErr, helpers.GetSlogErrorTags(args.OrgId, args.EntityName, args.BackfillRequestId, additionalKinesisTags))
				return workflowengine.NilActivityResult{}, wrappedErr
			}

			// Write the batch to Kinesis.
			err = a.writeBatchToKinesis(ctx, args, batchValue)
			if err != nil {
				monitoring.AggregatedDatadog.Incr(streamEmrDataToKinesis_WriteBatchToKinesisError)
				wrappedErr := oops.Wrapf(err, "EMR Backfill: failed to write batch to kinesis data stream")
				slog.Errorw(ctx, wrappedErr, helpers.GetSlogErrorTags(args.OrgId, args.EntityName, args.BackfillRequestId, additionalKinesisTags))
				return workflowengine.NilActivityResult{}, wrappedErr
			}

			totalRecords += int64(len(batchItems))
		}

		slog.Infow(ctx, "EMR Backfill: Wrote batch to kinesis", helpers.GetSlogInfoTags(args.OrgId, args.EntityName, args.BackfillRequestId, slog.Tag{"totalRecords": totalRecords, "pageToken": pageToken, "batchIndex": args.BatchIndex})...)
		// If there's no next page token, we're done with this batch of asset IDs.
		if getEmrDataResultResp.NextPageToken == nil {
			break
		}

		// Update page token for next iteration.
		pageToken = getEmrDataResultResp.NextPageToken
	}

	return workflowengine.NilActivityResult{}, nil
}

type getEmrDataAResult struct {
	EmrData       value.Value
	NextPageToken *string
}

func (a StreamEmrDataToKinesisActivity) getEmrData(ctx context.Context, args *StreamEmrDataToKinesisActivityArgs) (getEmrDataAResult, error) {
	startTimeMs := samtime.TimeToMs(a.Clock.Now())
	authzPrincipalId := &entityproto.AuthzPrincipalId{
		InternalServiceName: pointer.New(appenv.AppName()),
	}
	entityMap, err := a.EntityMetadataAccessor.GetFullEntityMap(ctx, args.OrgId)
	if err != nil {
		monitoring.AggregatedDatadog.Incr(streamEmrDataToKinesis_GetEmrData_GetEntityMapError)
		return getEmrDataAResult{}, oops.Wrapf(err, "failed to get entity map")
	}

	entity := entityMap.Get(args.EntityName)
	if entity == nil {
		monitoring.AggregatedDatadog.Incr(streamEmrDataToKinesis_GetEmrData_GetEntityFromMapError)
		return getEmrDataAResult{}, oops.Errorf("entity not found for name: %s", args.EntityName)
	}

	// Check if this entity supports parameterOptions (parameterized entity).
	listRecordsApi := entity.GetApiEndpoint(entityproto.ApiEndpoint_ListRecords)
	parametersField := listRecordsApi.RequestType().Field(entityconstants.ApiRequestParametersField)
	isParameterizedEntity := parametersField != nil && parametersField.Type().AsObject() != nil

	// Build filters if AssetIds is provided or if we need time-based filtering
	var filtersBuilder *value.ObjectBuilder
	filtersField := listRecordsApi.RequestType().Field(entityconstants.ApiRequestFiltersField)
	if filtersField != nil && filtersField.Type().AsObject() != nil {
		filtersBuilder = value.NewObjectBuilder(filtersField.Type().AsObject())

		// Add asset ID filters if provided Skip asset filtering for parameterized
		// entities (they use time-range queries instead)
		if len(args.AssetIds) > 0 && !isParameterizedEntity {
			// Use the FilterId from the EMR replication registry to get the correct filter field
			if args.FilterId == "" {
				return getEmrDataAResult{}, oops.Errorf("entity %s does not have FilterId specified in EMR replication registry", entity.Name())
			}

			// Get the filter field using the FilterId
			assetIdsInField := filtersField.Type().AsObject().Field(args.FilterId)
			if assetIdsInField == nil {
				return getEmrDataAResult{}, oops.Errorf("entity %s filter field %s not found in filters object", entity.Name(), args.FilterId)
			}

			assetIdsInType := assetIdsInField.Type()
			assetIdType := assetIdsInType.AsArray().ItemType()
			assetIdValues := make([]value.Value, len(args.AssetIds))
			for i, id := range args.AssetIds {
				assetIdValue, err := value.NewStringValue(assetIdType, fmt.Sprintf("%d", id))
				if err != nil {
					return getEmrDataAResult{}, oops.Wrapf(err, "failed to create asset ID value")
				}
				assetIdValues[i] = assetIdValue
			}

			assetIdArray, err := value.NewArrayValue(assetIdsInType, assetIdValues...)
			if err != nil {
				return getEmrDataAResult{}, oops.Wrapf(err, "failed to build asset ID array")
			}

			if err := filtersBuilder.SetFieldByName(args.FilterId, assetIdArray); err != nil {
				return getEmrDataAResult{}, oops.Wrapf(err, "failed to set %s filter", args.FilterId)
			}
		}
		// Add time-based filters if provided (for non-parameterized entities).
		if args.Filters != nil && !isParameterizedEntity {
			for fieldName, fieldValue := range args.Filters {
				timeField := filtersField.Type().AsObject().Field(fieldName)
				if timeField == nil {
					return getEmrDataAResult{}, oops.Errorf("entity %s is missing an expected filter field named %s", entity.Name(), fieldName)
				}
				timeValue, err := value.NewIntegerValue(timeField.Type(), fieldValue)
				if err != nil {
					return getEmrDataAResult{}, oops.Wrapf(err, "failed to create value for start time %d", *args.StartMs)
				}

				if err := filtersBuilder.SetField(timeField, timeValue); err != nil {
					return getEmrDataAResult{}, oops.Wrapf(err, "failed to set trip start time filter field")
				}
			}
		}
	}

	// Build parameters for parameterized entities (e.g., HosViolation).
	var parametersBuilder *value.ObjectBuilder
	if isParameterizedEntity {
		parametersBuilder = value.NewObjectBuilder(parametersField.Type().AsObject())

		// Add startMs parameter.
		if args.StartMs != nil {
			startMsField := parametersField.Type().AsObject().Field("startMs")
			if startMsField != nil {
				startMsValue, err := value.NewIntegerValue(startMsField.Type(), *args.StartMs)
				if err != nil {
					return getEmrDataAResult{}, oops.Wrapf(err, "failed to create startMs parameter value")
				}
				if err := parametersBuilder.SetField(startMsField, startMsValue); err != nil {
					return getEmrDataAResult{}, oops.Wrapf(err, "failed to set startMs parameter")
				}
			}
		}

		// Add endMs parameter (using current time if not provided).
		endMs := samtime.TimeToMs(a.Clock.Now())
		if args.EndMs != nil {
			endMs = *args.EndMs
		}
		endMsField := parametersField.Type().AsObject().Field("endMs")
		if endMsField != nil {
			endMsValue, err := value.NewIntegerValue(endMsField.Type(), endMs)
			if err != nil {
				return getEmrDataAResult{}, oops.Wrapf(err, "failed to create endMs parameter value")
			}
			if err := parametersBuilder.SetField(endMsField, endMsValue); err != nil {
				return getEmrDataAResult{}, oops.Wrapf(err, "failed to set endMs parameter")
			}
		}
	}

	queryArgs := &entitydataaccessor.QueryArguments{}
	if filtersBuilder != nil {
		queryArgs.Filters = filtersBuilder
	}
	if parametersBuilder != nil {
		queryArgs.CustomParameters = parametersBuilder
	}

	// Get a single page of results.
	data, nextPageToken, err := a.EntityDataAccessor.GetUnjoinedEntitiesPaginated(ctx, args.OrgId, authzPrincipalId, entity, queryArgs, args.PageToken)
	if err != nil {
		monitoring.AggregatedDatadog.Incr(streamEmrDataToKinesis_GetEmrData_GetUnjoinedEntitiesError)
		return getEmrDataAResult{}, oops.Wrapf(err, "failed to get unjoined entities page")
	}

	endTimeMs := samtime.TimeToMs(a.Clock.Now())
	duration := float64(endTimeMs - startTimeMs)
	monitoring.AggregatedDatadog.HistogramValue(duration, streamEmrDataToKinesis_GetEmrData_Duration)

	return getEmrDataAResult{
		EmrData:       data,
		NextPageToken: nextPageToken,
	}, nil
}

func (a StreamEmrDataToKinesisActivity) writeBatchToKinesis(ctx context.Context, args *StreamEmrDataToKinesisActivityArgs, emrData value.Value) error {

	startTimeMs := samtime.TimeToMs(a.Clock.Now())
	// Validate that we have data to write.
	if emrData.IsNil() {
		return oops.Errorf("emrData is nil")
	}

	// Convert EMR data to array.
	emrDataArray, err := emrData.AsArray()
	if err != nil {
		return oops.Wrapf(err, "failed to convert emrData to object")
	}
	cell := samsaraaws.GetECSClusterName()
	streamName := fmt.Sprintf("emr_replication_backfill_kinesis_data_streams_%s", cell)
	// Create Kinesis records.
	items := emrDataArray.Items()
	records := make([]*kinesis.PutRecordsRequestEntry, 0, len(items))
	slog.Infow(ctx, "EMR Backfill: Starting to write batch to kinesis", "items", len(items), "orgId", args.OrgId, "entityName", args.EntityName, "backfillRequestId", args.BackfillRequestId)
	for _, item := range items {
		kinesisRecordItem, err := a.constructKinesisRecord(ctx, args, item)
		if err != nil {
			return oops.Wrapf(err, "failed to construct kinesis record")
		}
		// Marshal the item to JSON.
		jsonData, err := json.Marshal(kinesisRecordItem)
		if err != nil {
			return oops.Wrapf(err, "failed to marshal item to JSON")
		}

		// Create a partition key that combines entity name, org ID, and backfill request ID.
		partitionKey := fmt.Sprintf("%s-%d-%s-%d", args.EntityName, args.OrgId, args.BackfillRequestId, samtime.TimeToMs(kinesisRecordItem.ReceivedAt))
		records = append(records, &kinesis.PutRecordsRequestEntry{
			Data:         jsonData,
			PartitionKey: aws.String(partitionKey),
		})
	}

	slog.Infow(ctx, "EMR Backfill: Sending batch to kinesis", "numRecords", len(records), "orgId", args.OrgId, "entityName", args.EntityName, "backfillRequestId", args.BackfillRequestId)
	// Send records to Kinesis.
	err = a.sendBatchToKinesis(ctx, streamName, records)
	if err != nil {
		monitoring.AggregatedDatadog.Incr(streamEmrDataToKinesis_SendBatchToKinesisError)
		return oops.Wrapf(err, "failed to send batch to Kinesis")
	}
	endTimeMs := samtime.TimeToMs(a.Clock.Now())
	duration := float64(endTimeMs - startTimeMs)
	monitoring.AggregatedDatadog.HistogramValue(duration, streamEmrDataToKinesis_WriteBatchToKinesis_Duration)

	slog.Infow(ctx, "EMR Backfill: Finished writing batch to kinesis", "numRecords", len(records), "orgId", args.OrgId, "entityName", args.EntityName, "backfillRequestId", args.BackfillRequestId)
	return nil
}

func (a StreamEmrDataToKinesisActivity) sendBatchToKinesis(ctx context.Context, streamName string, records []*kinesis.PutRecordsRequestEntry) error {
	startTimeMs := samtime.TimeToMs(a.Clock.Now())

	// Handle empty input
	if len(records) == 0 {
		return nil
	}

	// Validate record sizes before sending to Kinesis
	for i, record := range records {
		if len(record.Data) > maxRecordSize {
			monitoring.AggregatedDatadog.Incr(streamEmrDataToKinesis_SendBatchToKinesis_RecordExceedsSizeLimitError)
			return oops.Errorf("record at index %d exceeds Kinesis size limit of 1MB (size: %d bytes)", i, len(record.Data))
		}
	}

	// Split records into batches that respect the 5MB PutRecords limit
	batches := a.splitIntoBatches(records)

	// Send each batch - sendSingleBatch retries until all records succeed or returns an error.
	for batchIdx, batch := range batches {
		err := a.sendSingleBatch(ctx, streamName, batch, batchIdx)
		if err != nil {
			return oops.Wrapf(err, "failed to send batch %d/%d", batchIdx+1, len(batches))
		}
	}

	endTimeMs := samtime.TimeToMs(a.Clock.Now())
	duration := float64(endTimeMs - startTimeMs)
	monitoring.AggregatedDatadog.HistogramValue(duration, streamEmrDataToKinesis_SendBatchToKinesis_Duration)

	return nil
}

// splitIntoBatches splits records into batches that respect the maxBatchSize limit (5MB)
func (a StreamEmrDataToKinesisActivity) splitIntoBatches(records []*kinesis.PutRecordsRequestEntry) [][]*kinesis.PutRecordsRequestEntry {
	var batches [][]*kinesis.PutRecordsRequestEntry
	currentBatch := make([]*kinesis.PutRecordsRequestEntry, 0, len(records))
	currentBatchSize := 0

	for _, record := range records {
		// Calculate the full record size including data, partition key, and
		// overhead The Kinesis PutRecords API 5MB limit applies to the entire
		// request payload.
		// Documentation: https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html
		recordSize := len(record.Data)
		if record.PartitionKey != nil {
			recordSize += len(*record.PartitionKey)
		}
		// Add overhead for request structure (conservatively estimated).
		recordSize += 100

		// Log if a single record exceeds the batch size limit (5MB)
		// This is currently prevented by maxRecordSize (1MB), but track it for future changes
		if recordSize > maxBatchSize {
			monitoring.AggregatedDatadog.Incr(streamEmrDataToKinesis_SplitIntoBatches_RecordExceedsBatchSizeLimit)
		}

		// If adding this record would exceed the batch size limit, start a new batch
		if currentBatchSize+recordSize > maxBatchSize && len(currentBatch) > 0 {
			batches = append(batches, currentBatch)
			currentBatch = make([]*kinesis.PutRecordsRequestEntry, 0, len(records))
			currentBatchSize = 0
		}

		currentBatch = append(currentBatch, record)
		currentBatchSize += recordSize
	}

	// Add the last batch if it has any records
	if len(currentBatch) > 0 {
		batches = append(batches, currentBatch)
	}

	return batches
}

// sendSingleBatch sends a single batch of records to Kinesis with retry logic
func (a StreamEmrDataToKinesisActivity) sendSingleBatch(ctx context.Context, streamName string, records []*kinesis.PutRecordsRequestEntry, batchIdx int) error {
	remainingIndices := make([]int, len(records))
	for i := range remainingIndices {
		remainingIndices[i] = i
	}

	attempt := 0
	for len(remainingIndices) > 0 && attempt < maxRetries {
		if attempt > 0 {
			// Exponential backoff with jitter
			backoffMs := int64(backoffBase * (1 << attempt)) // 100ms, 200ms, 400ms...
			jitter := rand.Int63n(backoffMs / 2)
			time.Sleep(time.Duration(backoffMs+jitter) * time.Millisecond)
		}

		// Build batch from remaining indices
		remainingRecords := make([]*kinesis.PutRecordsRequestEntry, len(remainingIndices))
		for i, idx := range remainingIndices {
			remainingRecords[i] = records[idx]
		}

		output, err := a.KinesisClient.PutRecordsWithContext(ctx, &kinesis.PutRecordsInput{
			StreamName: aws.String(streamName),
			Records:    remainingRecords,
		})
		if err != nil {
			monitoring.AggregatedDatadog.Incr(streamEmrDataToKinesis_SendBatchToKinesis_PutRecordsError)
			return oops.Wrapf(err, "failed to put records to Kinesis stream")
		}

		if output.FailedRecordCount == nil || *output.FailedRecordCount == 0 {
			// All records in this attempt succeeded
			return nil
		}

		// Collect failed records for retry
		var newRemainingIndices []int
		for i, record := range output.Records {
			originalIdx := remainingIndices[i]
			if record.ErrorCode != nil {
				// When we encounter this ProvisionedThroughputExceededException error,
				// it's possible for some records in the batch to be successfully
				// written while other records fail due to throttling. Records that were
				// written have a SequenceNumber attribute, while failed records do not.
				if *record.ErrorCode == "ProvisionedThroughputExceededException" {
					// Only retry if we don't have a sequence number, which means this
					// record originally failed.
					if record.SequenceNumber == nil {
						newRemainingIndices = append(newRemainingIndices, originalIdx)
					}
					// Records with SequenceNumber succeeded despite the error code
				} else {
					monitoring.AggregatedDatadog.Incr(streamEmrDataToKinesis_SendBatchToKinesis_NonThrottlingError)
					// If we encounter a non-throttling error, fail immediately
					return oops.Errorf("record %d failed with error: %s - %s",
						originalIdx, *record.ErrorCode, *record.ErrorMessage)
				}
			}
			// Records without ErrorCode succeeded
		}

		remainingIndices = newRemainingIndices
		attempt++
	}

	// If we still have failed records after all retries
	if len(remainingIndices) > 0 {
		monitoring.AggregatedDatadog.Incr(streamEmrDataToKinesis_SendBatchToKinesis_FailedRecordsAfterRetries)
		return oops.Errorf("failed to write %d records to Kinesis after %d attempts",
			len(remainingIndices), maxRetries)
	}

	return nil
}

func (a StreamEmrDataToKinesisActivity) constructKinesisRecord(ctx context.Context, args *StreamEmrDataToKinesisActivityArgs, emrData value.Value) (*kinesisRecord, error) {
	kinesisRecordItem := &kinesisRecord{
		OrgId:             fmt.Sprintf("%d", args.OrgId),
		BackfillRequestId: args.BackfillRequestId,
		EmrResponse:       &emrData,
		EntityName:        args.EntityName,
		ChangeOperation:   emrworker.ChangeOperationCreateOrUpdate,
		ReceivedAt:        a.Clock.Now(),
	}
	// Marshal the item to JSON.
	jsonData, err := json.Marshal(kinesisRecordItem)
	if err != nil {
		return nil, oops.Wrapf(err, "failed to marshal item to JSON")
	}

	// Check if the record would exceed Kinesis size limit.
	if len(jsonData) > maxRecordSize {
		// Record is too large, offload to S3.
		s3Path, err := a.uploadToS3(ctx, &emrData, args.OrgId, args.EntityName, args.BackfillRequestId, samtime.TimeToMs(kinesisRecordItem.ReceivedAt))
		if err != nil {
			monitoring.AggregatedDatadog.Incr(
				streamEmrDataToKinesis_S3_Offload_Error,
				"entity_name:"+args.EntityName,
			)
			return nil, oops.Wrapf(err, "failed to upload large record to S3")
		}
		// Create S3 reference record.
		kinesisRecordItem.S3Path = s3Path
		// Remove the large data.
		kinesisRecordItem.EmrResponse = nil
	}

	return kinesisRecordItem, nil
}

func (a StreamEmrDataToKinesisActivity) uploadToS3(ctx context.Context, emrData *value.Value, orgId int64, entityName string, backfillRequestId string, timestamp int64) (string, error) {
	// Generate hash from EMR data to avoid collisions
	dataHash, err := helpers.GenerateEmrDataHash(emrData)
	if err != nil {
		return "", oops.Wrapf(err, "failed to generate hash for EMR data")
	}

	// Marshal the EMR data to JSON.
	emrBytes, err := json.Marshal(emrData)
	if err != nil {
		return "", oops.Wrapf(err, "failed to marshal EMR data for S3 upload")
	}
	s3Bucket, err := helpers.GetS3ExportBucketName()
	if err != nil {
		return "", oops.Wrapf(err, "failed to get S3 export bucket name")
	}
	s3Key := fmt.Sprintf("backfill/large-data/%s/%s/%d/%d_%s.json", entityName, backfillRequestId, orgId, timestamp, dataHash)
	_, err = a.S3Client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s3Bucket),
		Key:         aws.String(s3Key),
		Body:        strings.NewReader(string(emrBytes)),
		ContentType: aws.String("application/json"),
		Metadata: map[string]*string{
			"entity-name":         aws.String(entityName),
			"org-id":              aws.String(fmt.Sprintf("%d", orgId)),
			"backfill-request-id": aws.String(backfillRequestId),
		},
	})
	if err != nil {
		return "", oops.Wrapf(err, "failed to upload large EMR Response data to S3")
	}

	// Increment the metric for successful S3 upload
	monitoring.AggregatedDatadog.Incr(
		streamEmrDataToKinesis_S3_Offload_Success,
		"entity_name:"+entityName,
	)

	slog.Infow(ctx, "EMR Backfill: Successfully uploaded large EMR data to S3",
		"s3_bucket", s3Bucket,
		"s3_key", s3Key,
		"data_size", len(emrBytes),
		"data_hash", dataHash,
		"entity_name", entityName,
		"org_id", orgId,
		"backfill_request_id", backfillRequestId,
	)

	// Return the full S3 URL for the offloaded data to be sent as part of the
	// kinesis record.
	fullS3URL := fmt.Sprintf("s3://%s/%s", s3Bucket, s3Key)
	return fullS3URL, nil
}
