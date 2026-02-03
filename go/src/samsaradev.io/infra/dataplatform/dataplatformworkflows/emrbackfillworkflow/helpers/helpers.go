package helpers

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"time"

	"samsaradev.io/helpers/slog"
	"samsaradev.io/infra/samsaraaws"
	"samsaradev.io/infra/workflows/client/workflowengine"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/platform/entity/types/value"
)

// calculateBatchJitter generates a jitter value between 0 and maxSeconds based on the batch index and total batches.
// This ensures an even distribution of jitter across all batches.
func CalculateBatchJitter(batchIndex, totalBatches int, maxSeconds int) time.Duration {
	// If there are no batches or max seconds is 0, return 0 jitter
	if totalBatches == 0 || maxSeconds == 0 {
		return 0
	}

	// Create a hash of the batch index and total batches to get a deterministic but seemingly random value
	h := fnv.New32a()
	h.Write([]byte{byte(batchIndex), byte(totalBatches)})
	hashValue := h.Sum32()

	// Use the hash value to generate a jitter between 0 and maxSeconds We use
	// modulo to ensure the value is within our desired range
	jitterSeconds := int(hashValue % uint32(maxSeconds))
	return time.Duration(jitterSeconds) * time.Second
}

// batchChildWorkflowExecutions splits child workflow executions into batches of
// specified size. It returns a slice of batches, where each batch is a slice of
// child workflow executions.
func BatchChildWorkflowExecutions[T any](executions []workflowengine.ChildWorkflowExecution[T], batchSize int) [][]workflowengine.ChildWorkflowExecution[T] {
	if batchSize <= 0 {
		return [][]workflowengine.ChildWorkflowExecution[T]{executions}
	}

	batches := make([][]workflowengine.ChildWorkflowExecution[T], 0)
	for i := 0; i < len(executions); i += batchSize {
		end := i + batchSize
		if end > len(executions) {
			end = len(executions)
		}
		batches = append(batches, executions[i:end])
	}
	return batches
}

// getSlogTags returns a slice of interface{} containing the slog info tags for
// the given arguments.
func GetSlogInfoTags(orgId int64, entityName string, backfillRequestId string, tags slog.Tag) []interface{} {
	result := make([]interface{}, 0)
	for k, v := range tags {
		result = append(result, k, v)
	}

	return append([]interface{}{
		"orgId", orgId,
		"entityName", entityName,
		"backfillRequestId", backfillRequestId,
	}, result...)
}

func GetSlogErrorTags(orgId int64, entityName string, backfillRequestId string, tags slog.Tag) slog.Tag {
	defaultTags := slog.Tag{
		"orgId":             orgId,
		"entityName":        entityName,
		"backfillRequestId": backfillRequestId,
	}

	// Merge the default tags with any additional tags provided
	mergedTags := mergeMaps(defaultTags, tags)

	// Create a new map for the slog.Tag
	tagMap := make(slog.Tag)
	for k, v := range mergedTags {
		tagMap[k] = v
	}

	return tagMap
}

// Merge two maps into a new map.
func mergeMaps(map1, map2 map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	// Copy all entries from map1
	for k, v := range map1 {
		result[k] = v
	}

	// Copy all entries from map2 (will overwrite any duplicate keys from map1)
	for k, v := range map2 {
		result[k] = v
	}

	return result
}

// GenerateEmrDataHash generates a deterministic hash from EMR data to use in S3 keys
// to avoid collisions when multiple records are processed at the same timestamp.
func GenerateEmrDataHash(emrData *value.Value) (string, error) {
	// Marshal the EMR data to JSON for consistent byte representation
	emrBytes, err := json.Marshal(emrData)
	if err != nil {
		return "", fmt.Errorf("failed to marshal EMR data for hashing: %w", err)
	}

	// Use FNV hash for consistent, fast hashing
	h := fnv.New64a()
	h.Write(emrBytes)
	hashValue := h.Sum64()

	// Return hash as hex string (16 characters)
	return fmt.Sprintf("%016x", hashValue), nil
}

func GetS3ExportBucketName() (string, error) {
	prefix := ""
	switch samsaraaws.GetAWSRegion() {
	case infraconsts.SamsaraAWSDefaultRegion:
		prefix = infraconsts.SamsaraClouds.USProd.S3BucketPrefix
	case infraconsts.SamsaraAWSEURegion:
		prefix = infraconsts.SamsaraClouds.EUProd.S3BucketPrefix
	case infraconsts.SamsaraAWSCARegion:
		prefix = infraconsts.SamsaraClouds.CAProd.S3BucketPrefix
	default:
		return "", fmt.Errorf("unknown region: %s", samsaraaws.GetAWSRegion())
	}
	return fmt.Sprintf("%semr-replication-export-%s", prefix, samsaraaws.GetECSClusterName()), nil
}
